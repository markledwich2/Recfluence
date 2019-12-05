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

# helpfull articles
# simple explanation http://neoacademic.com/2011/11/16/computing-intraclass-correlations-icc-as-estimates-of-interrater-reliability-in-spss/
# specific to irr https://www.r-bloggers.com/k-is-for-cohens-kappa/
# another irr one.maybe cappa is better http://www.cookbook-r.com/Statistical_analysis/Inter-rater_reliability/

results = data.frame(tag=character(), subjects=integer(), raters=integer(), 
                     kap=numeric(), kap_p=numeric(), 
                      icc=numeric(), icc_p=numeric(), icc_lbound=numeric(),icc_ubound=numeric(), 
                        agreement=numeric())

addResult <- function(name, ratings) {
  agree = agree(ratings)
  kap = kappam.fleiss(ratings)
  icc = icc(ratings, model="twoway", type="agreement")
  df = data.frame(name=name, subjects=kap$subjects, raters=kap$raters, 
                  kap = kap$value, kap_p = kap$p.value,
                  icc = icc$value, icc_p = icc$p.value, icc_lbound=icc$lbound, icc_ubound=icc$ubound,
                  agreement = agree$value)
  rbind(results, df)
}



#
# lr ICC
#

lr = getResult("icc_lr")

reviwers = c("Ac", "os", "zY") #  filter out reviewers with only a handfull of classifications
lrCodes = c( "L" = -1, "C" = 0, "R" = 1)

lrRatings = lr %>% 
  filter(REVIEWER %in% reviwers) %>%
  mutate(REVIEWER_LR = lrCodes[REVIEWER_LR]) %>%
  spread(REVIEWER, REVIEWER_LR, sep="_") %>%
  select(starts_with("REVIEWER"))


results = addResult("Left/Center/Right", lrRatings)


#
# tag ICC
#
tag = getResult("icc_tags")
uniqueTags = unique(tag$TAG)

for(t in uniqueTags) {
  tagRatings = tag %>%
    filter(REVIEWER %in% reviwers & TAG == t) %>%
    mutate(REVIEWER_HAS_TAG = as.logical(REVIEWER_HAS_TAG)) %>%
    spread(REVIEWER, REVIEWER_HAS_TAG, sep="_") %>%
    select(starts_with("REVIEWER"))
  
  results = addResult(t, tagRatings)
}

print(results)

write.csv(results, "reviewer_reliability.csv", row.names = FALSE, sep = ",")

