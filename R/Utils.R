# Util Funcs

# ssl_join

#' @export
ssl_join <- function(ssl1, ssl2, ssl1_ratio) {
  ssl_mix <-
    dplyr::inner_join(
      ssl1 %>% select(date, stock_cd, pred_mean, target_1m_return),
      ssl2 %>% select(date, stock_cd, pred_mean),
      by=c("date", "stock_cd")
    ) %>%
    rename(ssl1=pred_mean.x, ssl2=pred_mean.y) %>%
    mutate(pred_mix = ssl1*ssl1_ratio + ssl2*(1-ssl1_ratio)) %>%
    select(date, stock_cd, ssl1, ssl2, pred_mix, target_1m_return)
  return(ssl_mix)
}
