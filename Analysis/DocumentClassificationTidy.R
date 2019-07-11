library(readtext)
library(dplyr)
library(tidyr)
library(rstudioapi)
library(tidytext)
options(stringsAsFactors = FALSE)
library(ggplot2)
library(tm)
library(topicmodels)
library(readr)


current_path <- getActiveDocumentContext()$path
setwd(paste(dirname(current_path), "/../Data/"))

channels <- read.csv(file="SeedChannels.csv", header=TRUE, sep=",", comment.char = '#', quote = "\"")


videos <- read_delim(file="Videos.csv.gz", delim=",", quote = "\"", , escape_double=FALSE, escape_backslash=TRUE) %>%
  mutate(Views = as.numeric(Views)) %>%
  mutate(path = paste("VideoCaptions/", ChannelId, "/", VideoId, ".txt", sep=""), exists = file.exists(path)) %>%
  filter(exists == TRUE) %>%
  left_join(channels %>% select (ChannelId=Id, ChannelTitle=Title))
  

paths <- videos %>% group_by(ChannelId) %>% top_n(100, Views) %>% ungroup() %>% pull(path)

docText <- bind_rows(lapply(paths, function(c) {  readtext(c, cache = FALSE, docvarsfrom = "filepaths", dvsep = "/", encoding = "UTF-8")  } )) %>%
  select(doc_id, text, docvar2) %>% 
  rename(channel = docvar2) %>%
  mutate(video = sub("\\..*", "", doc_id))

 
data(stop_words)
replaceTicks <- function(s) {
  
  # 1 character substitutions
  s1 <- chartr("šžþàáâãäåçèéêëìíîïðñòóôõöùúûüý", "szyaaaaaaceeeeiiiidnooooouuuuy", s)
  
  # 2 character substitutions
  old <- c("œ",  "ß",  "æ",  "ø")
  new <- c("oe", "ss", "ae", "oe")
  s2 <- s1
  for(i in seq_along(old)) s2 <- gsub(old[i], new[i], s2, fixed = TRUE)
  
  s2
}

myStopwords <- tibble(word= c("can", "say","one","way","use",
                 "also","howev","tell","will",
                 "much","need","take","tend","even",
                 "like","particular","rather","said",
                 "get","well","make","ask","come","end",
                 "first","two","help","often","may",
                 "might","see","someth","thing","point",
                 "post","look","right","now","think", "put","set","new","good",
                 "want","sure","kind","larg","yes,","day","etc",
                 "quit","sinc","attempt","lack","seen","awar",
                 "littl","ever","moreov","though","found","abl",
                 "enough","far","earli","away","achiev","draw",
                 "last","never","brief","bit","entir","brief",
                 "great","lot", "know", "just", "mean", "thats",
                 "yeah", "gonna", "theyre", "really", "people",
                 "dont", "youre", "going", "okay", "theres",
                 "got", "hes", "yes", "shes", "says", "something", "im",
                 "ive", "didnt", "talking", "time", "whats", "doesnt",
                 "talk", "ill", "youve", "isnt",  "weve", "person", "pretty", "guys", "um", "hey", "true" , "weve",  "theyve",
                 "called", "bad","uh","wait" , "feel", "stuff", "guess",  
                  "hear", "heres", "wasnt", "fucking", "fuck", "shit",
                 "sort", "idea", "basically", "started", "start", "steve",
                 "ready", "amount", "aware", "ben", "youtube.com", "minds.com"))



tidyTxt <- docText %>%
  mutate(text = gsub(text, pattern = "[0-9]+|[[:punct:]]\x20-\x7E", replacement = "") %>% replaceTicks() ) %>%
  unnest_tokens(word, text)  %>%
  anti_join(stop_words) %>%
  anti_join(myStopwords)
  
videoWords <- tidyTxt %>%
   count(video, word)
#   left_join(videos %>% select(video=VideoId, ChannelId, Title)) %>%
#   left_join(channels %>% select(ChannelId=Id, ChannelTitle=Title)) %>%
#   filter(ChannelTitle == "Timcast")

channelWords <- tidyTxt %>%
  count(channel, word) %>%
  bind_tf_idf(word, channel, n) %>%
  arrange(desc(tf_idf))

topChannelWords <- channelWords %>%
  select(channel, word, tf_idf, n) %>%
  group_by(channel) %>%
  filter(n > 5) %>%
  top_n(100, tf_idf) %>%
  ungroup() %>%
  left_join(channels %>% select(channel = Id, Title)) %>%
  select(ChannelId = channel, Title, Word = word, TfIdf = tf_idf, Count = n)

channelStats <- channelWords %>%
  select(channel, word, tf_idf, n) %>%
  group_by(channel) %>%
  summarise(sum(n)) %>%
  left_join(channels %>% select(channel = Id, Title))

trainingVideos <- videos %>% pull(VideoId) #%>% group_by(ChannelId) %>% top_n(30, Views) %>% ungroup()


toDtm <- function (wordsFrame) {
  dtm <- wordsFrame %>% cast_dtm(video, word, n) %>% removeSparseTerms(0.95)
  dtmRowTotals <- apply(dtm , 1, sum) #Find the sum of words in each Document
  dtm[dtmRowTotals> 0, ]           #remove all docs without words
}

##%>% filter(tf_idf > median(tidyTxt$tf_idf)*2)
dtm <- toDtm(videoWords %>% filter(video %in% trainingVideos))

lda <- LDA(dtm, k = 20, control = list(seed = 1234, iter = 400), method = "Gibbs")

terms <- tidy(lda, matrix = "beta")
topics <- tidy(lda, matrix = "gamma")

topicWords <- terms %>% group_by(topic) %>% top_n(3, beta) %>% arrange(topic, desc(beta)) %>% summarise(label = paste0(term, collapse = " "))# %>% ungroup()

#dtmFull <- toDtm(videoWords %>% group_by(video) %>% top_n(10) %>% ungroup())
#topicsTermsMatrix <-topicmodels::posterior(lda, dtmFull)

getVideoTopcis <- function(topics) {
  topics %>%
    group_by(document) %>%
    top_n(1, gamma) %>%
    ungroup() %>%
    left_join(videos %>% select(document=VideoId, ChannelId, Title)) %>%
    left_join(channels %>% select(ChannelId=Id, ChannelTitle=Title)) %>%
    left_join(topicWords) %>%
    rename(VideoId=document)
}

videoTopics <- getVideoTopcis(topics)

channelTopics <- videoTopics %>% 
  rename(Topic=label) %>%
  group_by(ChannelTitle,ChannelId, Topic) %>% summarize(Videos=n()) %>% mutate(OnTopic = Videos / sum(Videos))

write.csv(videoTopics, file = "VideoTopics", row.names = FALSE, fileEncoding = "UTF8")
write.csv(topChannelWords, file = "./../Site/static/ChannelWords.csv", row.names = FALSE, fileEncoding = "UTF8")
write.csv(channelTopics , "./../Site/static/ChannelTopics.csv", row.names = FALSE, fileEncoding = "UTF8")


