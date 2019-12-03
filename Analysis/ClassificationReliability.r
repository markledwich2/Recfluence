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

# helpfull article https://www.r-bloggers.com/k-is-for-cohens-kappa/


iccResults = data.frame(tag=character(), subjects=integer(), raters=integer(), 
                        value=numeric(), p.value=numeric(), lbound=numeric(),ubound=numeric())

addResult <- function(name, cciList) {
  print(cciList)
  rbind(iccResults, data.frame(name=name, subjects=cciList$subjects, raters=cciList$raters, 
       value = cciList$value, p.value=cciList$p.value, lbound=cciList$lbound, ubound=cciList$ubound ))
}



#
# lr ICC
#

lr = getResult("icc_lr")

reviwers = c("Ac", "os", "zY") #  filter out reviewers with only a handfull of classifications
lrCodes = c( "L" = -1, "C" = 0, "R" = 1)

lrIcc = lr %>% 
  filter(REVIEWER %in% reviwers) %>%
  mutate(REVIEWER_LR = lrCodes[REVIEWER_LR]) %>%
  spread(REVIEWER, REVIEWER_LR, sep="_") %>%
  select(starts_with("REVIEWER"))


iccResults = addResult("Left/Center/Right", lrIcc %>% icc(model="twoway", type="agreement"))


#
# tag ICC
#
tag = getResult("icc_tags")
uniqueTags = unique(tag$TAG)

for(t in uniqueTags) {
  tagIcc = tag %>%
    filter(REVIEWER %in% reviwers & TAG == t) %>%
    mutate(REVIEWER_HAS_TAG = as.logical(REVIEWER_HAS_TAG)) %>%
    spread(REVIEWER, REVIEWER_HAS_TAG, sep="_") %>%
    select(starts_with("REVIEWER"))
  
  iccResults = addResult(t, tagIcc %>% icc(model="twoway", type="agreement"))
}

write.csv(iccResults, "reviewer_reliability.csv", row.names = FALSE, sep = ",")

