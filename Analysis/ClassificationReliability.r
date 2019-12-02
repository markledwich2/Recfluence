library(lpSolve)
library(irr)
library(dplyr)
library(tidyr)

options(stringsAsFactors = FALSE)

getResult <- function(name) {
  con <- gzcon(url(paste("https://pyt.blob.core.windows.net/data/results/v2/latest/", name, ".csv.gz", sep="")))
  txt <- readLines(con)
  return(read.csv(textConnection(txt), header=TRUE, quote="\""))
}

#
# lr ICC
#

lr = getResult("icc_lr")

agreementPercent = (lr %>% 
  mutate(REVIEWER_LR_NUM = LR == REVIEWER_LR) %>% 
  summarize(nAgeed = sum(REVIEWER_LR_NUM)/n()))

print(agreementPercent)

reviwers <- c("Ac", "os", "zY") #  filter out reviewers with only a handfull of classifications

lrIcc = lr %>% 
  filter(REVIEWER %in% reviwers) %>%
  mutate(REVIEWER_LR = switch(REVIEWER_LR, "L" = -1, "C" = 0, "R" = 1)) %>%
  spread(REVIEWER, REVIEWER_LR, sep="_") %>%
  select(starts_with("REVIEWER"))


# all of the different ways of calculating agreement look pretty bad
agree(lrIcc)
lrIcc %>% icc(model="twoway", type="agreement")
kappam.fleiss(lrIcc)
kappam.light(lrIcc)

#
# tag ICC
#
tag = getResult("icc_tags")


tagIcc = tag %>%
  filter(REVIEWER %in% reviwers) %>%
  mutate(REVIEWER_HAS_TAG = as.logical(REVIEWER_HAS_TAG)) %>%
  spread(REVIEWER, REVIEWER_HAS_TAG, sep="_") %>%
  select(starts_with("REVIEWER"))

agree(tagIcc)
tagIcc %>% icc(model="twoway", type="agreement")
kappam.fleiss(tagIcc)
kappam.light(tagIcc)
