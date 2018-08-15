library(caret)
data("GermanCredit")
write.csv(GermanCredit, "german_credit.csv")

library(chunked)
library(dplyr)
## Here we don't read the file, we just get something like a pointer to it.
data_chunked <- read_chunkwise('german_credit.csv', 
                               chunk_size = 100)
data_chunked

data_chunked %>% 
  summarise(n = n()) %>% # chunked will get the number of rows of each chunk
  as.data.frame() %>% # here we read the data returned from summarise()
  summarise(nrows = sum(n)) # and summarise() the length of each chunk

data_chunked %>%
  group_by(Class) %>%
  summarise(freq = n()) %>%
  as.data.frame() %>%
  group_by(Class) %>%
  summarise(freq = sum(freq))

data_chunked %>%
  select(Class, Age, 
         Personal.Male.Single, 
         ForeignWorker, NumberExistingCredits) %>%
  filter(Personal.Male.Single > 0 & 
           ForeignWorker > 0 & 
           NumberExistingCredits < 2) %>%
  group_by(Class) %>%
  summarise(mean_age = mean(Age)) %>%
  collect() %>% 
  group_by(Class) %>%
  summarise(mean_age = mean(mean_age))

library(ggplot2)
data_chunked %>% 
  select(Class, Amount, Duration) %>%
  collect() %>%
  ggplot(aes(x = Duration, y = Amount, color = Class)) +
  geom_jitter(alpha = 0.7) + 
  scale_color_brewer(palette = 'Set1') +
  theme_minimal() + 
  theme(legend.position = 'top')

data_chunked %>% 
  select(Age, ForeignWorker) %>%
  mutate(ForeignWorker = as.factor(ForeignWorker)) %>%
  collect() %>%
  ggplot(aes(x = Age, fill = ForeignWorker)) +
  geom_density(color = 'white', alpha = 0.6) +
  scale_x_continuous(breaks = seq(10, 100, 10)) +
  theme_minimal() + scale_fill_brewer(palette = 'Set1')

sampled <- data_chunked %>%
  mutate(rand = runif(100)) %>% ## 100 number of each chunk
  filter(rand < 0.1) %>%
  select(- rand) %>%
  as.data.frame()
dim(sampled)

library(sqldf)
query <- 'SELECT * FROM file ORDER BY RANDOM() LIMIT 100'
df <- read.csv.sql('german_credit.csv', 
                   sql = query)
dim(df)

indices <- data_chunked %>% 
  mutate(n = Purpose.Education > 0) %>%
  select(n) %>% 
  collect()
indices <- indices[, 1]
sum(indices)

library(data.table)
sequence  <- rle(indices)
index  <- c(0, cumsum(sequence$lengths))[which(sequence$values)] + 1
idx <- data.frame(start = index, 
                  length = sequence$length[which(sequence$values)])
head(idx)

data_fread <- do.call(
  rbind,
  apply(idx, 1, 
        function(x) return(fread(
          "german_credit.csv", nrows = x[2], skip = x[1]))))
dim(data_fread)

