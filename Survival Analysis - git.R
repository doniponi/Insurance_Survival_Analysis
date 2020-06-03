###########################################################################################################################
#####################################################Project Library#######################################################
###########################################################################################################################
library(dplyr)
library(lubridate)
library(RMariaDB)
library(sqldf)
library(stringr)
library(ggplot2)
library(plyr)
library(fastDummies)
library(caret)
library(eeptools)
library(cattonum)
library(mice) 
library(DMwR)
library(ROSE)
library(pROC)
library(survival)
library(survminer)

###########################################################################################################################
###################################################Read Table##############################################################
#Load Raw Data
# set memory limits
options(java.parameters = "-Xmx100000m") # 64048 is 64 GB
#install.packages("odbc")
#install.packages("RMariaDB")
# Connect to a MariaDB version of a MySQL database
con <- dbConnect(RMariaDB::MariaDB(), host="domain", port=number
                 , dbname="name"
                 , user="username", password="password")
# list of db tables
dbListTables(con)

# query tables
Claims <- dbGetQuery(con, "SELECT * FROM Claim_New")
Household<-dbGetQuery(con, 'SELECT * FROM Household')
Individual<-dbGetQuery(con, 'SELECT * FROM Individual')
IndividualHousehold<-dbGetQuery(con, 'SELECT * FROM IndividualHousehold')
Policy<-dbGetQuery(con, 'SELECT * FROM Policy')

# close db connection
suppressWarnings(dbDisconnect(con)); rm(con)

###########################################################################################################################
###################################################Policy##################################################################
#Policy dataset
#1. Subsetting policy into AUTO and HOME
Policy_A = subset(Policy, Policy$PolicyType == 'AUTO')
Policy_H = subset(Policy, Policy$PolicyType == 'HOME')

#2. Change the format of time from character to POSIXct
class(Policy$PolicyInceptionDate)
Policy_A$PolicyInceptionDate = ymd(Policy_A$PolicyInceptionDate)
Policy_A$PolicyEffectiveDate = ymd(Policy_A$PolicyEffectiveDate)
Policy_A$PolicyExpirationDate = ymd(Policy_A$PolicyExpirationDate)
Policy_A$PolicyCancellationDate = ymd(Policy_A$PolicyCancellationDate)

Policy_H$PolicyInceptionDate = ymd(Policy_H$PolicyInceptionDate)
Policy_H$PolicyEffectiveDate = ymd(Policy_H$PolicyEffectiveDate)
Policy_H$PolicyExpirationDate = ymd(Policy_H$PolicyExpirationDate)
Policy_H$PolicyCancellationDate = ymd(Policy_H$PolicyCancellationDate)

#3. Add new columns names diff1 & diff2
Policy_A$diff1 = Policy_A$PolicyExpirationDate - Policy_A$PolicyCancellationDate
Policy_A$diff2 = Policy_A$PolicyEffectiveDate - Policy_A$PolicyCancellationDate
Policy_A$diff3 = Policy_A$PolicyExpirationDate - Policy_A$PolicyEffectiveDate

Policy_H$diff1 = Policy_H$PolicyExpirationDate - Policy_H$PolicyCancellationDate
Policy_H$diff2 = Policy_H$PolicyEffectiveDate - Policy_H$PolicyCancellationDate
Policy_H$diff3 = Policy_H$PolicyExpirationDate - Policy_H$PolicyEffectiveDate

#4. Label the policy ('Active', 'Expired','Cancelled')
Policy_A$label = ifelse(Policy_A$PolicyStatus == 'InForce', 'Active', ifelse(is.na(Policy_A$diff1) == TRUE, 'Expired', ifelse(Policy_A$diff1 == 0 | Policy_A$diff2 == 0, 'Expired', 'Cancelled')))
Policy_H$label = ifelse(Policy_H$PolicyStatus == 'InForce', 'Active', ifelse(is.na(Policy_H$diff1) == TRUE, 'Expired', ifelse(Policy_H$diff1 == 0 | Policy_H$diff2 == 0, 'Expired', 'Cancelled')))
Policy = sqldf("SELECT *
                FROM Policy_A
                UNION
                SELECT *
                FROM Policy_H
               ", method = 'raw')

###########################################################################################################################
######################################################Claims###############################################################
#Convert date type
#str(Claims)
Claims$LossDate<-as.Date(parse_date_time(Claims$LossDate,orders="ymd HMS"))
Claims$ClosedDate<-as.Date(parse_date_time(Claims$ClosedDate,orders="ymd HMS"))
Claims$ReportedDate<-as.Date(parse_date_time(Claims$ReportedDate,orders="ymd HMS"))
Claims$CreatedDateTime<-as.Date(parse_date_time(Claims$CreatedDateTime,orders="ymd HMS"))

## drop index
Claims$index<-NULL

## get time diff
#identical(Claims$ReportedDate,Claims$CreatedDateTime)
Claims$diff_create_report<-Claims$CreatedDateTime-Claims$ReportedDate
Claims$diff_close_report<-Claims$ClosedDate-Claims$ReportedDate
Claims$diff_report_loss<-Claims$ReportedDate-Claims$LossDate

###### remove claim with prior insurer
Claims<-sqldf("SELECT * 
              FROM Claims as a
              LEFT JOIN (SELECT PolicyID, PolicyInceptionDate FROM Policy) as b
              ON a.PolicyID = b.PolicyID
              ", method = "raw")
Claims$prior<-ifelse(Claims$CreatedDateTime-Claims$PolicyInceptionDate<0,1,0)
Claims<-subset(Claims,prior==0)
Claims$PolicyID..14<-NULL
Claims$PolicyInceptionDate<-NULL
Claims$prior<-NULL

## subset closed cases (why we only keep closed cases, we assume customers with open cases will not leave)
Claims_closed<-subset(Claims, ClaimStatus == 'Closed'| ClaimStatus == 'Closed Due to Error')


###########################################################################################################################
###############################################Aggregate policy to individual level########################################
#Subset individual without any cancalled/expired policy
df = Policy
df$index = NULL
df[,c(9:15)] = NULL

#Find the last day of the Policy, no matter it is actived or not
df$Last_Day = ifelse(is.na(df$PolicyCancellationDate) == TRUE, df$PolicyExpirationDate, df$PolicyCancellationDate)
timeline = sqldf('SELECT IndividualID, PolicyID, PolicyInceptionDate, PolicyStatus, label, MAX(Last_Day) as Last_Day
            FROM df
            GROUP BY IndividualID
            ORDER BY Last_Day asc
           ', method = 'raw')
#Set the time period last day
timeline$Final = 18219-60 

#Set the label for each individual of churned(1) or not churned(0)
timeline$Status = ifelse(timeline$Last_Day > timeline$Final, 0, 1)

#Find first cancelled policy for each individual
temp1 = subset(df, label != 'Active')
temp2 = sqldf('SELECT a.IndividualID, MIN(Last_Day) as First_Day
               FROM temp1 as a
               GROUP BY IndividualID
               Order By Last_Day asc;
              ', method = 'raw')
#Subset the qualified individual (people with at least one cancelled/expired policy)
timeline = sqldf("SELECT b.*,a.First_Day
                  FROM temp2 as a
                  LEFT JOIN timeline as b
                  ON a.IndividualID = b.IndividualID
                 ", method = 'raw')

#Calculate the survival time in days
timeline$t = ifelse(timeline$Last_Day >= timeline$Final, timeline$Final - timeline$First_Day, timeline$Last_Day - timeline$First_Day)
timeline$t = ifelse(timeline$t < 0, 0, timeline$t)
timeline$t = round(timeline$t/30)
timeline$PolicyInceptionDate = NULL
timeline$PolicyStatus = NULL

#Merge the individual information
timeline = sqldf('SELECT a.*, b.Gender, b.MaritalStatus, b.NumberofDependents, b.AgeGroup, b.OtherState_Household, b.Relationship_Child
                  FROM timeline as a
                  LEFT JOIN Individual_New as b
                  ON a.IndividualID = b.IndividualID; 
                 ', method = 'raw')

#Find auto policy number and home policy number 
auto_number = sqldf('SELECT IndividualID, count(PolicyType) as count_A
                     FROM Policy_A
                     GROUP BY IndividualID
                    ', method = 'raw')
home_number = sqldf('SELECT IndividualID, count(PolicyType) as count_H
                     FROM Policy_H
                     GROUP BY IndividualID
                    ', method = 'raw')
timeline = sqldf('SELECT a.*, b.count_A
                  FROM timeline as a
                  LEFT JOIN auto_number as b
                  ON a.IndividualID = b.IndividualID;
                 ', method = 'raw')
timeline = sqldf('SELECT a.*, b.count_H
                  FROM timeline as a
                  LEFT JOIN home_number as b
                  ON a.IndividualID = b.IndividualID;
                 ', method = 'raw')
timeline$count_A = ifelse(is.na(timeline$count_A) == TRUE, 0, timeline$count_A)
timeline$count_H = ifelse(is.na(timeline$count_H) == TRUE, 0, timeline$count_H)

#Merge claim information
Claims_1<-sqldf("SELECT * 
                FROM Claims_closed as a
                LEFT JOIN (SELECT IndividualID, PolicyID from Policy) as b
                ON a.PolicyID=b.PolicyID
                ", method="raw")
Claims_2<-sqldf("SELECT IndividualID, count(ClaimNumber) as TotalClaimNumber, avg(TotalPayment) as AvgClaimPMT
                FROM Claims_1
                GROUP BY IndividualID
                ",method='raw')
timeline = sqldf('SELECT a.*, b.TotalClaimNumber, b.AvgClaimPMT
                  FROM timeline as a
                  LEFT JOIN Claims_2 as b
                  ON a.IndividualID=b.IndividualID
                 ',method = 'raw')
timeline$TotalClaimNumber = ifelse(is.na(timeline$TotalClaimNumber) == TRUE, 0, timeline$TotalClaimNumber)
timeline$AvgClaimPMT = ifelse(is.na(timeline$AvgClaimPMT) == TRUE, 0, timeline$AvgClaimPMT)
timeline$PolicyID = NULL
timeline$label = NULL
timeline$Last_Day = NULL
timeline$Final = NULL

#Label the claim
temp3 = sqldf('SELECT *
               FROM Claims_closed as a 
               LEFT JOIN df as b
               WHERE a.PolicyID = b.PolicyID
              ', method = 'raw')
temp4 = sqldf('SELECT b.ReportedDate, b.IndividualID, a.First_Day
               FROM temp2 as a 
               LEFT JOIN temp3 as b
               WHERE a.IndividualID = b.IndividualID
              ', method = 'raw')
temp4$diff =  temp4$First_Day - temp4$ReportedDate
temp4$claim_after = ifelse(temp4$diff < 0, 1, 0)
temp4$claim_before = ifelse(temp4$diff >= 0, 1, 0)

temp5 = sqldf('SELECT IndividualID, SUM(claim_after) as claim_after_count, SUM(claim_before) as claim_before_count
               FROM temp4
               GROUP BY IndividualID
              ', method = 'raw')
temp5$claim_after = ifelse(temp5$claim_after_count>0, 1, 0)
temp5$claim_before = ifelse(temp5$claim_before_count>0, 1, 0)

timeline = sqldf('SELECT a.*, b.*
                  FROM timeline as a
                  LEFT JOIN temp5 as b
                  ON a.IndividualID=b.IndividualID
                 ',method = 'raw')
timeline$IndividualID..15 = NULL
timeline$claim_after_count = ifelse(is.na(timeline$claim_after_count),0, timeline$claim_after_count)
timeline$claim_before_count = ifelse(is.na(timeline$claim_before_count),0, timeline$claim_before_count)
timeline$claim_after = ifelse(is.na(timeline$claim_after),0, timeline$claim_after)
timeline$claim_before = ifelse(is.na(timeline$claim_before),0, timeline$claim_before)

#Distinguish policyholder with only auto policy, home policy, and both
timeline$customer_type = ifelse(timeline$count_A>0 & timeline$count_H>0, 2, ifelse(timeline$count_A>0, 0,1))

#Add children info to each individual
timeline = sqldf('SELECT a.*, b.Relationship_Child as child
                   FROM timeline as a
                   LEFT JOIN Individual_New as b
                   ON a.IndividualID = b.IndividualID
                  ', method = 'raw')
timeline$Relationship_Child = NULL
#remove na value
timeline = na.omit(timeline)
timeline$First_Day = NULL
timeline$IndividualID = NULL
timeline = subset(timeline, timeline$t > 0)

###########################################################################################################################
#####################################################K-M Survival Estimate####################################################
#create target y
timeline$y<-Surv(timeline$t,timeline$Status)
head(timeline)

#Using survival fit to do Kaplan-Meier Estimation
km<-survfit(y~AgeGroup,data = timeline)
km
quantile(km,probs =1- c(0.80,0.70,0.60))

summary(km,censored = T)

#Survival Curve (fun = "event" to plot cumulative curve)
ggsurvplot(km,
           pval = TRUE, conf.int = FALSE,
           risk.table = FALSE, # Add risk table
           risk.table.col = "strata", # Change risk table color by groups
           linetype = "strata", # Change line type by groups
           surv.median.line = "hv", # Specify median survival
           ggtheme = theme_bw(), #/.ka   Change ggplot2 theme
           xlab = 'Survival Time (Month)',
           size = 0.4,
           font.x = 10,
           font.y = 10,
           risk.table.height = 0.5,
           #palette = c("#AA852D",	"#868686")
)

#Log-Rank test
surv_diff <- survdiff(y ~ AgeGroup, data = timeline)
surv_diff

###########################################################################################################################
#######################################################Cox Regression######################################################
#One-Way Cox regression to Multi-Way Cox regression
covariates <- c("Gender","MaritalStatus","NumberofDependents","AgeGroup","OtherState_Household","count_A", "count_H", 
                "TotalClaimNumber","AvgClaimPMT","claim_before_count", "claim_after", "claim_before", "customer_type", "child", "claim_after_count")

#Creating list including each factor
univ_formulas <- sapply(covariates,
                        function(x) as.formula(paste('y~', x)))
univ_formulas

univ_models <- lapply(univ_formulas, function(x){coxph(x, data = timeline)})
univ_models

#Empty regression line, only include the intercept
cox.null<-coxph(y~1,data = timeline)
summary(cox.null)

#Full regression line
cox.full<-coxph(y~.-Status-t,data = timeline)
summary(cox.full)

#Stepwise
cox.both<-step(cox.null,
               scope = list(lower=formula(cox.null),upper=formula(cox.full)),
               direction = 'both',
               trace = T)
summary(cox.both)

#Create Linear Predictor Risk Score (The linear predictor for a specific set of covariates is the log-hazard-ratio 
#relative to a hypothetical (and very possibly non-existent) case with the mean of all the predictor values.)
timeline$lp <- predict(cox.both, type = "lp")

#ANOVA for feature
anova(cox.both,test = 'Chisq')

#Cox Regression Assumption Check (proportional hazard)
cox.zph(cox.both)

#Failed the assumption, therefore, using step functions to make different coefficient over different time period
#Create step function for time dependent coefficient
timeline_new <- survSplit(Surv(t, Status)~ ., data= timeline, cut=c(6, 12, 24), episode= "tgroup", id="id")

#Based on cox model, do the backward selection and forward selection to find the optimal model 
cox.1 <- coxph(Surv(tstart, t, Status) ~ count_A:strata(tgroup)+count_H:strata(tgroup)
               +OtherState_Household+child:strata(tgroup)+AgeGroup
               +claim_before:strata(tgroup), data=timeline_new, ties=c("breslow"))
cox.zph(cox.1)
plot(cox.zph(cox.both),var = 1)

summary(cox.1)

#Cox Regression Performance (ROC)
library(survivalROC)
timeline_new$lp <- predict(cox.1, type = "lp")
Survival_ROC= survivalROC(Stime=timeline_new$t,  
                          status=timeline_new$Status,      
                          marker = timeline_new$lp,     
                          predict.time = 24, method="KM")

plot(Survival_ROC$FP, Survival_ROC$TP, 
     type="l",col="red",xlim=c(0,1), ylim=c(0,1),   
     xlab=paste( "FP", "\n", "AUC = ",round(Survival_ROC$AUC,3)), 
     ylab="TP",
     main="Risk Score (Linear Predictor), Method = KM \n Month = 24")
abline(0,1,col="gray",lty=2)



