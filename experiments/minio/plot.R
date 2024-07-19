library(tidyverse)
c01 <- read_csv("results_consumer_minio_1_to_64/results_consumer_minio_1_to_192_c01")
c02 <- read_csv("results_consumer_minio_1_to_64/results_consumer_minio_1_to_192_c02")
c03 <- read_csv("results_consumer_minio_1_to_64/results_consumer_minio_1_to_192_c03")
c04 <- read_csv("results_consumer_minio_1_to_64/results_consumer_minio_1_to_192_c04")
c05 <- read_csv("results_consumer_minio_1_to_64/results_consumer_minio_1_to_192_c05")
combined_throughput <- tibble(
  numConsumers = c01$numConsumers,
  mps = c01$mps + c02$mps + c03$mps + c04$mps + c05$mps,
  fps = c01$fps + c02$fps + c03$fps + c04$fps + c05$fps,
)
ggplot(
  data = combined_throughput,
  mapping = aes(x = numConsumers, y = mps)
) + geom_line()

