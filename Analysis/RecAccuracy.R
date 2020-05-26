library(dplyr)
library(ggplot2)
library(Matrix)
library(irr)
library(tidyr)
library(ggplot2)
library(glue)

options(scipen = 999)  # disable sci notation

# pulls latest results from azure
getResult <- function(name) {
  con <- gzcon(url(paste("https://pyt.blob.core.windows.net/data/results/latest/", name, ".csv.gz", sep="")))
  txt <- readLines(con)
  return(read.csv(textConnection(txt), header=TRUE, quote="\""))
}

data.raw = getResult("rec_accuracy")

# comparison of actual vs estimate at the from_channel (all) + to_channel (channels that provided analytics) + month granularity
data.channel = data.raw %>%
  select(month=FROM_DATE, from_group=FROM_IDEOLOGY, to_group=TO_IDEOLOGY, estimate=IMPRESSIONS_ESTIMATE, actual=IMPRESSIONS_ACTUAL, 
         to_channel=TO_CHANNEL_HASH, from_channel=FROM_CHANNEL_HASH) %>%
  mutate(estimate = as.double(estimate), actual = as.double(actual), 
         error_pct = ifelse(estimate == 0 & actual == 0, 0, abs(estimate - actual)/ifelse(actual==0, estimate, actual))*100, 
         error_rank = rank(error_pct, ties.method = "first"))

# ranked order correlation per to_chaneel's actuals
# not enough samples
results.ranked_p_channel = data.channel %>% filter(actual > 0) %>% group_by(to_channel) %>%
  group_modify(~ {
    print(glue('number of rows for channel {.y} - {nrow(.x)}'))
    tibble::enframe(cor.test(x=.x$estimate, y=.x$actual, method = 'spearman'))
  })


# export data only includes top 500 incomming videos. 
# this means only the top-x (usually about 40-60 channels in our data set)
# consider our dataset as having 2 categories of data Inc & Exc. Did we get that right?
data.channel %>% group_by(to_channel) %>%
  group_modify(~ {
    min_actual = min((. %>% filter(actual > 0) )$actual)
    with_cats = data.channel %>% select(from_channel, to_channel, inc=actual > 0, inc_est=estimate > )
    tibble::enframe()
  })
#data.channel_cats = data.channel %>% mutate(inc=actual > 0, inc_est=estimate > )


# ranked order correlation  for all data
#data.channel_actual_only = data.channel %>% filter(actual > 0)
#data.channel_ranked = data.frame(cbind(rank(data.channel_actual_only$estimate, ties.method = 'average'),
#                               rank(data.channel_actual_only$actual, ties.method = 'average')))
#results.ranked_p = cor.test(x=data.channel_actual_only$estimate, y=data.channel_actual_only$actual, method = 'spearman')

# granularity: to_channel + month 
data.to_channel = data.channel  %>%
  group_by(month, to_channel) %>% summarize(estimate=sum(estimate), actual=sum(actual))


#graph the errors at the channel combo + month level
ggplot(data.channel %>% filter(actual > 0), aes(x=error_rank, y=error_pct)) + geom_point()
ggplot(data.channel %>% filter(actual > 0 & error_pct < 300 & error_pct != 100), aes(x=error_pct)) + geom_histogram()


# granulaity.: group combo + month
# Error metrics calculate at this level, because this is the level we publish analysis at.
# error_pct_togroup_month_avg is a the median error as a % of the average actual for that to_group.
# Not happy with this, or anything other ways I ahve tried to summarize the error
data.group =  data.channel %>% group_by(to_group, month) %>% 
  mutate(actual_togroup_month_total=sum(actual), actual_togroup_month_avg=mean(actual)) %>% ungroup %>%
  group_by(from_group, to_group, month) %>%
  summarise(estimate=sum(estimate), actual=sum(actual), 
            actual_togroup_month_total=first(actual_togroup_month_total), 
            actual_togroup_month_avg=first(actual_togroup_month_avg)) %>%
  rowwise %>% mutate(error = abs(estimate - actual),
            error_pct_togroup_month_avg = coalesce(error / actual_togroup_month_avg, 0)*100) %>% ungroup %>%
  mutate(error_rank=rank(error_pct_togroup_month_avg))


# result: the average error of estimate as a % of actual at the group combo + month granularity 
results.group_month_median_error_pct_avg = (data.group %>% summarize(median = median(error_pct_togroup_month_avg)))$median

# plot the error at the group combo + month level
ggplot(data.group, aes(x=error_rank, y=error_pct_togroup_month_avg)) + geom_point()
ggplot(data.group, aes(x=error_pct_togroup_month_avg)) + geom_histogram()

# icc for gorup combos. observations are group + date combos. reviewers are estaimte & actual
result.group_icc = icc(data.group %>% select(estimate, actual), model="twoway", type="agreement")
print(icc.group)


# icc for each to_channel. observations are channel combos. reviewers are estiamte & actual
result.channel_icc = data.channel %>% group_by(to_channel) %>% 
  group_modify(~ icc(.x %>% select(estimate, actual), model="twoway", type="agreement") %>% tibble::enframe()) %>%
  pivot_wider(to_channel, names_from=name, values_from = value) %>%
  select(to_channel, subjects, raters, value, p.value, lbound, ubound)
