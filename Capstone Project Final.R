##Loading Raw Data to R

#Step 1: Load JSON libraries to read data
library(rjson)
library(RJSONIO)
library(jsonlite)
library(doParallel)
library(foreach)
#Set WD
setwd("E:/Users/Philip Coyne/Documents/yelp_dataset_challenge_academic_dataset")

#Load Data into appropriate names

#The following utilizes the functions from libraries "jsonlite"
#It simplifies the data into a dataframe.  Data might be lost, but
#it is there.

yelpBusiness<-stream_in(file("yelp_academic_dataset_business.json"))
yelpCheckin<-stream_in(file("yelp_academic_dataset_checkin.json"))
yelpReview<-stream_in(file("yelp_academic_dataset_review.json"))
yelpTip<-stream_in(file("yelp_academic_dataset_tip.json"))
yelpUser<-stream_in(file("yelp_academic_dataset_user.json"))

#Code for running parallel processing
detectCores()
cl<-makeCluster(3)
registerDoParallel(cl)
getDoParWorkers()
#

placeHolder<-cbind(yelpUser$name,yelpUser$user_id)

reviewNumbers<-as.integer()
tipNumber<-as.integer()

#Clock parallel processing time for comparison
strt<-Sys.time()
number<-foreach(i=1:length(placeHolder[,2]))%dopar%which(yelpReview$user_id==placeHolder[i,2])
tipNumbers<-foreach(i=1:length(placeHolder[,2]))%dopar%which(yelpTip$user_id==placeHolder[i,2])


for(i in 1:length(number)){
  tipNumber[i]<-length(tipNumbers[[i]])
  reviewNumbers[i]<-length(number[[i]])
}
print(Sys.time()-strt)
newPlaceDF<-data.frame(placeHolder,tipNumber,reviewNumbers)
colnames(newPlaceDF)[1]<-"User Name"
colnames(newPlaceDF)[2]<-"User_ID"
colnames(newPlaceDF)[3]<-"Tips"
colnames(newPlaceDF)[4]<-"Reviews"

numbersDF<-as.data.frame

##Parallel processing takes about 1/3rd the time

##To be used as needed, Store dataframes into local
##SQL server if something happens to RStudio 
library(RODBC)
odbcName<-"FirstODBC"
myConn<-odbcConnect(odbcName)

navigateAdventure<-sqlQuery(myConn,"use AdventureWorks2014")
myConn<-odbcConnect(odbcName)
sqlSave(myConn,starBizDF,tablename="AdjustedBusinessRating",rownames = FALSE)
close(myConn)
##End code for SQL server storage



#Task 1: Filter out Business IDs, Review IDs, Review Text, and Date Submitted
##Task 1a: Find health-care providers & Doctors
findHealthMed<-grepl("Health & Medical",yelpBusiness$categories)
test<-yelpBusiness[findHealthMed,]
findDoctorFromHealthMed<-grepl("Doctors",test$categories)
doctorsTest<-test[findDoctorFromHealthMed,]


##Task 1b: Find reviews associated with business IDs

docBizID<-doctorsTest$business_id
docBizName<-doctorsTest$name
doctorsReviews<-data.frame(Name=character(),Business_ID=character(), Review_ID=character(), Review_Text=character(),City=character(),Stars=numeric())

for(i in 1:length(docBizID)){
  Business_ID<-docBizID[i]
  Name<-docBizName[i]
  preRI<-which(yelpReview$business_id==docBizID[i])
  if (length(preRI)>0){
    
    
    Review_ID<-yelpReview$review_id[preRI]
    City<-doctorsTest$city[i]
    Stars<-as.numeric()
    Review_Text<-as.character()
    #Task 1bA: Due to multiple reviews, we will need to make a 
    #nested for loop to find all the revies
    for (t in 1:length(Review_ID)){
      textHolder<-subset(yelpReview$text,yelpReview$review_id==Review_ID[t])
      Review_Text[t]<-textHolder
      starHolder<-subset(yelpReview$stars,yelpReview$review_id==Review_ID[t])
      Stars[t]<-starHolder
      
    }
    ammendment<-data.frame(cbind(Name,Business_ID,Review_ID,Review_Text,City,Stars))
    doctorsReviews<-rbind(doctorsReviews,ammendment)
  }
  
}
#Task 1c: Take dataframe and save to SQL DB
library(RODBC)
odbcName<-"FirstODBC"
myConn<-odbcConnect(odbcName)

navigateAdventure<-sqlQuery(myConn,"use AdventureWorks2014")
myConn<-odbcConnect(odbcName)
sqlSave(myConn,doctorsReviews,tablename="DoctorReviewsYelp",rownames = FALSE)
close(myConn)

#Task 2: Conduct exploratory modeling utilizing the code provided by My Coyne
#Task 2a: Separate doctor and health medical by city
cityList<-unique(doctorsReviews$City)
Phoenix<-subset(doctorsReviews,doctorsReviews$City==cityList[1])
Tolleson<-subset(doctorsReviews,doctorsReviews$City==cityList[43])
Edinburgh<-subset(doctorsReviews,doctorsReviews$City==cityList[31])
Champaign<-subset(doctorsReviews,doctorsReviews$City==cityList[6])
LasVegas<-subset(doctorsReviews,doctorsReviews$City==cityList[27])
Pittsburgh<-subset(doctorsReviews,doctorsReviews$City==cityList[2])
Pineville<-subset(doctorsReviews,doctorsReviews$City==cityList[3])
Charlotte<-subset(doctorsReviews,doctorsReviews$City==cityList[4])

inputText<-Phoenix$Review_Text
inputText<-Tolleson$Review_Text
inputText<-Edinburgh$Review_Text
inputText<-Champaign$Review_Text
inputText<-LasVegas$Review_Text
inputText<-Pittsburgh$Review_Text
inputText<-doctorsReviews$Review_Text

cityList<-unique(doctorsReviews$City)
Pittsburgh<-subset(doctorsReviews,doctorsReviews$City==cityList[2])
inputText<-Pittsburgh$Review_Text
##Task 3: Take input text and pass it through functions found 
##in package "tm"

library('RODBC')
library(jsonlite)

library(ggplot2)
library(tm)
library(SnowballC)
require(wordcloud)
require("topicmodels")
library (RWeka)
library(lsa)

library(proxy)
library(arules)
library(stats)

df.corpus <- Corpus(VectorSource(inputText))

##Task 3a: Set up stopword list
myStopwords <- c(stopwords("english"),"hes","shes","theyll","ive","get"
                 ,"also","dr","one","two","three","four","five","six","seven"
                 ,"eight","nine","ten","need","day","days","year","years","month","months","want","cant","can"
                 ,"didnt","dont","got","told","went","said","will","just","try","even","say"
                 ,"made","like","look","took","however","need","were","back","really","around"
                 ,"doctor","doctors","place","pittsburgh","upmc","need","take","make");

##Task 3b: Clean corpus

df.corpus.clean <- clean(df.corpus, myStopwords)
dtm <- DocumentTermMatrix(df.corpus.clean);
tdm<-TermDocumentMatrix(df.corpus.clean);


dtm.sp <- removeSparseTerms(dtm, sparse=0.80)
tdm.sp <- removeSparseTerms(tdm,sparse=0.80)

##Task 3c: Filter through matrices to root out frequent
##terms, sparse terms, and terms that don't exists in any
##document.
##Added Code 10/29/15

dtm.mat<- as.matrix(dtm.sp);
tdm.mat<-as.matrix(tdm.sp)

##For TermDocumentMatrix TDM.sp
##If a word shows up in >=99% of docs or <=1% of docs
rowMaxThreshold<-ceiling(dtm.sp$nrow*0.99)
rowMinThreshold<-ceiling(dtm.sp$nrow*0.01)

##For DocumentTermMatrix DTM.sp
colMaxThreshold<-ceiling(tdm.sp$ncol*0.99)
colMinThreshold<-ceiling(tdm.sp$ncol*0.01)

#Does it show up in 99% of documents?

#DTM
if(sum(colSums(dtm.mat)>rowMaxThreshold)>0){
  temp<-which(colSums(dtm.mat)>rowMaxThreshold)
  newWords<-names(temp)
  myStopwords<-c(myStopwords,newWords)
}

#TDM
if(sum(rowSums(tdm.mat)>colMaxThreshold)>0){
  temp<-which(colSums(tdm.mat)>colMaxThreshold)
  newWords<-names(temp)
  myStopwords<-c(myStopwords,newWords)
  mostFreqWords<-newWords
}

#Does it show up in less than 1% of documents?
if(sum(colSums(dtm.mat)<rowMinThreshold)>0){
  temp<-which(colSums(dtm.mat)>rowMinThreshold)
  newWords<-names(temp)
  myStopwords<-c(myStopwords,newWords)
}

if(sum(rowSums(tdm.mat)<colMinThreshold)>0){
  temp<-which(colSums(dtm.mat)>rowMinThreshold)
  newWords<-names(temp)
  myStopwords<-c(myStopwords,newWords)
}

#Add any new stop words to myStopwords and re-run
df.corpus.clean<-clean(df.corpus,myStopwords)

dtm <- DocumentTermMatrix(df.corpus.clean);
tdm<-TermDocumentMatrix(df.corpus.clean);
dtm.sp <- removeSparseTerms(dtm, sparse=0.80)
dtm.mat<-as.matrix(dtm.sp)
tdm.sp<-removeSparseTerms(tdm,sparse=0.80)

#Are there any words that don't show up in any documents?
#This must be run to perform LSA
if(sum(rowSums(dtm.mat)==0)>0){
  rowsToBeRM<-which(rowSums(dtm.mat)==0)
  dtm.sp<-dtm.sp[-rowsToBeRM,]
}

if(sum(colSums(tdm.mat)==0)>0){
  colsToBeRM<-which(colSums(tdm.mat)==0)
  tdm.sp<-tdm.sp[-colsToBeRM,]
}
#ONLY FOR PHOENIX
PhoenixNu<-Phoenix[-rowsToBeRM,]

dtm.mat <- as.matrix(dtm);
freq<-colSums(dtm.mat);
freq<-sort(freq, decreasing = TRUE)
words<-names(freq)
wordcloud(words,freq, scale=c(8,.2),min.freq=200,
          max.words=Inf, random.order=FALSE, rot.per=.15
          , colors=brewer.pal(8,"Dark2"))


dtm.mat <- as.matrix(dtm.sp);
freq<-colSums(dtm.mat);
freq<-sort(freq, decreasing = TRUE)
words<-names(freq)
wordcloud(words,freq, scale=c(8,.2),min.freq=200,max.words=Inf, random.order=FALSE, rot.per=.15,colors=brewer.pal(8,"Dark2"))


##Task 4: Pass dtm to LDA functions for analysis

##Task 4a: Determine the appropriate number of topics through
##Log-Likelihood
library(topicmodels)
set.seed(4)
burnin = 1000;
iter = 1000;
keep = 50;

sequ <- seq(2,50, by=1);
fitted <- lapply(sequ, function(k) {LDA(dtm.sp, method="Gibbs", k=k,LDA_Gibbscontrol=list(burnin=burnin, iter=iter, keep=keep))});
topicModel <- lapply(seq(2, 50, by = 1), function(d){LDA(dtm.sp, method="Gibbs",k=d, LDA_Gibbscontrol=list(burnin=burnin, iter=iter, keep=keep))}) 
logTopicModel <- as.data.frame(as.matrix(lapply(topicModel, logLik)))  
finalTopicModel <- data.frame(topics=seq(2, 50, by = 1), LL = as.numeric(as.matrix(logTopicModel)))

ggplot(finalTopicModel, aes(x = topics, y = LL)) + 
  xlab("Number of topics") + 
  ylab("Log likelihood of the model") + 
  geom_line() + 
  theme_bw()  + 
  theme(axis.title.x = element_text(vjust = -0.5, size = 14)) + 
  theme(axis.title.y=element_text(size = 14, angle=90, vjust= -0.25)) 

sortedLogs <- finalTopicModel[order(-finalTopicModel$LL), ] 
sortedLogs 
nTopics <- sortedLogs[1,]$topics

## Task 4b: After finding optimal number of topics, 
nTopics <- 9
myK = nTopics
myAlpha = (1/myK)

burnin = 1000;
iter = 1000;
keep = 50;
fit<-LDA(dtm.sp,k=myK,num.iterations=iter,alpha=myAlpha,eta=myAlpha,burnin=burnin,compute.log.likelihood=TRUE)
fittedR <-  LDA(dtm.sp, method="Gibbs", k=myK,LDA_Gibbscontrol=list(burnin=burnin, iter=iter, keep=keep));
doc_topic <- data.frame (lda_topic= topics(fittedR))
topic_term <- data.frame(lda_term = terms(fittedR))
doc_term  <- data.frame(prop_id=Pittsburgh$Review_ID, doc_topic, lda_terms = topic_term$lda_term[doc_topic$lda_topic])
## Task 4c: Display the topics and rankings
terms(fittedR,10)

##Task 5: Use LSA to determine how closely words are associated
##with each other


##Task 5a: Create vector space to determine association between terms across all documents
lsaSpace<-lsa(tdm.sp,dims=dimcalc_raw())

##dims=dimcalc_raw() and dim=2 yield same results for pittsburgh
#This will reconstruct the tdm.sp as a matrix
round(lsaSpace$tk %*% diag(lsaSpace$sk)%*% t(lsaSpace$dk))
newLSASpace<-lsa(tdm.sp,dims=2)
newMatrix<-round(as.textmatrix(newLSASpace),2)
t.locs<-newLSASpace$tk%*%diag(newLSASpace$sk)
plot(t.locs,type="p")
text(t.locs,labels=rownames(newLSASpace$tk))

##Task 5b: Create vector to contain categories and utilize doc_term$lda_terms
##Note this is going to be a lot of manual work
wordRows<-data.frame(words=as.character(),clusterNumber=as.integer())
tdm.mat<-as.matrix(tdm.sp)
words<-rownames(tdm.mat)



clusterNumber<-c("Clust 0","Clust 3","Clust 2","Clust 2","Clust 2","Clust 0","Clust 2","Clust 0","Clust 1","Clust 2","Clust 2","Clust 3","Clust 2","Clust 1","Clust 0","Clust 2","Clust 3","Clust 3","Clust 2","Clust 3","Clust 3","Clust 1","Clust 2","Clust 2","Clust 2","Clust 2","Clust 2","Clust 2", "Clust 0", "Clust 2", "Clust 3") 
wordRows<-cbind(words,clusterNumber)
wordRows<-as.data.frame(wordRows)
theLDATerms<-doc_term$lda_terms
ourLDATerms<-doc_term$lda_terms
uniqueLDATerms<-unique(ourLDATerms)
for(i in 1:length(uniqueLDATerms)){
  ourLDATerms<-doc_term$lda_terms
  logicalThing<-grepl(uniqueLDATerms[i],wordRows$words)
  replacement<-wordRows[logicalThing,]$clusterNumber
  theLDATerms<-sub(uniqueLDATerms[i],replacement=replacement,theLDATerms)
}
##Task 5c: Determine association between documents and categorize
##them according to word clusters.  

dist.mat<-dist(t(as.matrix(tdm.mat)))
dist.mat

fit<-cmdscale(dist.mat,eig=TRUE,k=2)
points<-data.frame(x = fit$points[, 1], y = fit$points[, 2])

ggplot(points,aes(x=x,y=y))+
  geom_point(data=points,aes(x=x,y=y,color=theLDATerms))+
  geom_text(data=points,aes(x=x,y=y-0.2,label=row.names(doc_term)))

