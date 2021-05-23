
# Prepare Data for Modeling

#' @export
df_prep_model = function(df, var1 = 'target_1m_updown_index', var2 = 'target_1m_updown_10', beg_date = '2011-01-01'){

  formatted = df %>%
    mutate(date = ymd(date)) %>%
    mutate(stock_cd = str_pad(stock_cd, 6,side = c('left'), pad = '0')) %>%
    filter(date >= as.Date(beg_date)) %>%
    filter(!is.na(get(var1))) %>%
    filter(!is.na(get(var2))) %>% as.data.frame()

  return(formatted)

}
