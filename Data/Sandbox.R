library("dplyr", lib.loc="~/R/win-library/3.5")

Channels %>%
  select(Title, Id, SubCount) %>%
  arrange(desc(SubCount))
