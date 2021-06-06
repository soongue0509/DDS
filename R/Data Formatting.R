
# Prepare Data for Modeling

#' @export
df_prep_model = function(df, beg_date = '2011-01-01'){

  formatted = df %>%
    as.data.frame() %>%
    mutate(date = ymd(date)) %>%
    mutate(stock_cd = str_pad(stock_cd, 6,side = c('left'), pad = '0')) %>%
    filter(date >= as.Date(beg_date)) %>% 
    as.data.frame()

  return(formatted)

}
