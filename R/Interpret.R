# Interpret Models and Results

#' @export
return_tile = function(df, ssl_input, pred_col) {

  if(!"target_1m_return" %in% colnames(ssl_input)){
    stop("target_1m_return must exist in ssl")
  }

  library(RMySQL)
  stock_db_connection <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db'
  )
  dbSendQuery(stock_db_connection, "SET NAMES utf8;")
  dbSendQuery(stock_db_connection, "SET CHARACTER SET utf8mb4;")
  dbSendQuery(stock_db_connection, "SET character_set_connection=utf8mb4;")

  # Get KOSPI KOSDAQ
  d_kospi_kosdaq = dbGetQuery(stock_db_connection, paste0("select * from stock_kospi_kosdaq where date >= '20051201'"))

  # Get Market Updown
  market_updown <-
    df %>%
    select(date) %>%
    unique() %>%
    left_join(d_kospi_kosdaq %>% mutate(date = ymd(date)) %>% select(date, kospi, kosdaq), by ="date") %>%
    mutate(kospi = (lead(kospi)-kospi)/kospi,
           kosdaq = (lead(kosdaq)-kosdaq)/kosdaq) %>%
    mutate(market_avg = (kospi+kosdaq)/2) %>%
    as_tibble() %>%
    na.omit()

  # Top 30 and KOSPI/KOSDAQ ======
  pvm <-
    ssl_input %>%
    group_by(date) %>%
    arrange(desc(get(pred_col)), .by_group = T) %>%
    na.omit() %>%
    dplyr::slice(1:30) %>%
    group_by(date) %>%
    summarize(portfolio_return = mean(target_1m_return)) %>%
    left_join(market_updown %>% select(date, kospi, kosdaq, market_avg), by="date")

  pvm %>%
    mutate(date = fct_rev(factor(date))) %>%
    gather(gubun, value, -date) %>%
    mutate(value = round(value*100, 3)) %>%
    mutate(gubun = case_when(gubun == 'kospi' ~ paste0(gubun, '\n corr : ', round(cor(pvm$portfolio_return, pvm$kospi),4)),
                             gubun == 'kosdaq' ~ paste0(gubun, '\n corr : ', round(cor(pvm$portfolio_return, pvm$kosdaq),4)),
                             gubun == 'market_avg' ~ paste0(gubun, '\n corr : ', round(cor(pvm$portfolio_return, pvm$market_avg),4)),
                             TRUE ~ paste0(gubun, '\n cumret : ', round(tail(cumprod(pvm$portfolio_return+1)-1, 1), 4)))) %>%
    mutate(gubun = factor(gubun, levels=c(paste0('portfolio_return\n cumret : ', round(tail(cumprod(pvm$portfolio_return+1)-1, 1), 4)),
                                          paste0('kospi', '\n corr : ', round(cor(pvm$portfolio_return, pvm$kospi),4)),
                                          paste0('kosdaq', '\n corr : ', round(cor(pvm$portfolio_return, pvm$kosdaq),4)),
                                          paste0('market_avg', '\n corr : ', round(cor(pvm$portfolio_return, pvm$market_avg),4))))) %>%
    ggplot(aes(y=date, x=gubun, fill=value, label = paste0(value, '%'))) +
    geom_tile(color='black', size=0.25) +
    geom_text(size=3) +
    scale_fill_gradient2(
      low = "blue3",
      mid = "white",
      high = "red3",
      midpoint = 0,
      space = "Lab",
      na.value = "grey50",
      guide = "colourbar",
      aesthetics = "fill"
    ) +
    theme_minimal() +
    theme(axis.title.x=element_blank())
}
