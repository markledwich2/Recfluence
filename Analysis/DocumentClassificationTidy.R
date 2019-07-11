library(readtext)
library(dplyr)
library(tidyr)
library(rstudioapi)
library(tidytext)
options(stringsAsFactors = FALSE)
library(ggplot2)
library(tm)

current_path <- getActiveDocumentContext()$path
setwd(paste(dirname(current_path), "/../Data/"))

readText <- readtext(".VideoCaptions/*/*.txt", cache = FALSE, 
                     docvarsfrom = "filepaths", dvsep = "/", encoding = "UTF-8")
channels <- read.csv(file="SeedChannels.csv", header=TRUE, sep=",", comment.char = '#')

docText <- readText %>%
  select(doc_id, text, docvar2) %>% 
  rename(channel = docvar2 ) %>%
  group_by(channel) %>% 
  summarise(text = paste(text, collapse ="\n"))
  mutate


removeSpecialChars <- function(x) gsub("["."-'-???.]","",x)

stopwords <- stopwords("english")

txt <- SimpleCorpus(DataframeSource(as.data.frame(docText %>% rename(doc_id = channel)))) %>% 
  tm_map(removePunctuation) %>%
  tm_map(removeNumbers) %>%
  tm_map(removeSpecialChars) %>%
  #tm_map(removeWords, stopwords) %>%
  DocumentTermMatrix %>%
  tidy %>%
  rename(channel = document) %>%
  bind_tf_idf(term, channel, count) %>%
  arrange(desc(tf_idf))

topChannelWords <- txt %>%
  select(channel, term, tf_idf, count) %>%
  group_by(channel) %>%
  filter(count > 10) %>%
  top_n(500, tf_idf) %>%
  ungroup() %>%
  left_join(channels %>% select(channel = Id, Title)) %>%
  select(ChannelId = channel, Title, Word = term, TfIdf = tf_idf, Count = count)

channelStats <- txt %>%
  select(channel, word, tf_idf, n) %>%
  group_by(channel) %>%
  summarise(sum(n)) %>%
  left_join(channels %>% select(channel = Id, Title))



write.csv(topChannelWords, file = "./../Site/static/ChannelWords.csv", row.names = FALSE)

medianTfIdf = median(txt$tf_idf)
txt <- txt %>% filter(tf_idf > medianTfIdf)
dtm <- txt %>% cast_dtm(channel, word, n)



lda <- LDA(dtm, k = 4, control = list(seed = 1234))
topics <- tidy(lda, matrix = "beta")


topTerms <- topics %>%
  group_by(topic) %>%
  top_n(20, beta) %>%
  ungroup() %>%
  arrange(topic, -beta)

topTerms %>%
  mutate(term = reorder(term, beta)) %>%
  ggplot(aes(term, beta, fill = factor(topic))) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~ topic, scales = "free") +
  coord_flip()


channelTopics <- tidy(lda, matrix = "gamma") %>% 
  group_by(document) %>%
  top_n(1, gamma) %>%
  ungroup()

write.csv(channelTopics %>% select(ChannelId = document, Topic = topic), "ChannelTopics.csv", row.names = FALSE)

channels <- channels %>%
  left_join(channelTopics %>% select(document, topic), by = c("Id" = "document")) 


