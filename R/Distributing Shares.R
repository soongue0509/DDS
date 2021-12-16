# Distributing Shares

# How many shares

#' @export
how_many_shares = function(ssl, seed_money, pred_col, topN=30, SN_ratio=0.3, min_transaction_amount=1e8, view_method="long") {
  Sys.setlocale("LC_CTYPE", "ko_KR.UTF-8")
  if(length(unique(ssl$date)) != 1) {
    stop("There must be only one date in SSL.")
  }
  
  inv_date <- str_replace_all(unique(ssl$date),'-','')
  print(paste0("Inv Date: ", inv_date))
  
  # Read Data
  d_stock_price_temp <- 
    dbConnect(MySQL(),
              user = 'betterlife',
              password = 'snail132',
              host = 'betterlife.duckdns.org',
              port = 1231 ,
              dbname = 'stock_db') %>% 
    dbGetQuery(paste0("select * from stock_adj_price where date = '", inv_date,"';")) %>%
    select(stock_cd, date, price = adj_close_price)
  
  ssl_temp =
    ssl %>%
    ta_filtering(min_transaction_amount) %>%
    sector_neutral(SN_ratio, topN, pred_col) %>% 
    arrange(desc(get(pred_col))) %>%
    dplyr::slice(1:topN) %>%
    left_join(d_stock_price_temp %>% mutate(date=ymd(date)), by=c("stock_cd", "date")) %>%
    mutate(each_stock_cap = seed_money / topN) %>%
    mutate(cnt_temp = floor(each_stock_cap / price)) %>%
    mutate(amt_temp = price * cnt_temp)
  
  surplus = seed_money - sum(ssl_temp$amt_temp)
  
  k=1
  while (unique(surplus) > min(ssl_temp$price)) {
    
    if (k > topN) {
      k = 1
    } else {
      k = k
    }
    
    if(surplus >= ssl_temp$price[k]) {
      ssl_temp$cnt_temp[k] = ssl_temp$cnt_temp[k] + 1
      surplus = surplus - ssl_temp$price[k]
      k = k + 1
    } else {
      k = k + 1
    }
  }
  
  final_result =
    ssl_temp %>%
    mutate(proportion = paste0(round((cnt_temp*price) / sum(cnt_temp*price) * 100, 2), '%'), amt=price*cnt_temp) %>%
    select(stock_cd, stock_nm, market, all_of(pred_col), cnt=cnt_temp, amt, proportion, last_close_price=price)
  
  print("============================================================================")
  print(paste0("Surplus: ", surplus))
  
  lapply(dbListConnections(dbDriver(drv = "MySQL")), dbDisconnect)
  
  if (view_method == "long") {
    return(final_result)
  } else {
    return(t(final_result))
  }
}

# SHAP for chosen stocks

#' @export
explain_why = function(shap, topN, macro_yn=FALSE) {
  
  if(length(unique(shap$date)) != 1) {
    stop("There must be only one date in SHAP data.")
  }
  
  stock_db_connection <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231 ,
    dbname = 'stock_db'
  )
  dbSendQuery(stock_db_connection, "SET NAMES utf8;")
  dbSendQuery(stock_db_connection, "SET CHARACTER SET utf8mb4;")
  dbSendQuery(stock_db_connection, "SET character_set_connection=utf8mb4;")
  
  inv_date = str_replace_all(unique(shap$date),'-','')
  
  stock_nm = dbGetQuery(stock_db_connection, paste0("select stock_cd, stock_nm from stock_market_sector where date = '", inv_date,"';"))
  
  if (macro_yn == TRUE) {
    temp <- 
      shap %>% 
      select(date, stock_cd, variable, value, rfvalue, pred_mean)
  } else {
    feature_list <- dbGetQuery(stock_db_connection, "select * from feature_list")
    temp <- 
      shap %>% 
      select(date, stock_cd, variable, value, rfvalue, pred_mean) %>% 
      left_join(feature_list %>% filter(category == 'Macro') %>% select(feature, category), by=c("variable"="feature")) %>% 
      filter(is.na(category)) %>% 
      select(-category)
  }
  
  lapply(dbListConnections(dbDriver(drv = "MySQL")), dbDisconnect)
  
  plot_df <-
    temp %>% 
    inner_join(
      temp %>% 
        select(date, stock_cd, pred_mean) %>% 
        unique() %>% 
        arrange(desc(pred_mean)) %>% 
        dplyr::slice(1:topN) %>% 
        select(-pred_mean),
      by=c("date", "stock_cd")
    ) %>% 
    left_join(stock_nm, by="stock_cd") %>% 
    mutate(stock_cd_f = factor(paste0(stock_cd, ' / ', stock_nm)))
  
  plot_df %>% 
    group_by(stock_cd) %>% 
    top_n(6, abs(value)) %>% 
    ungroup() %>% 
    arrange(stock_cd, value) %>%
    mutate(order = row_number(),
           shap_sign = ifelse(value > 0, "pos", "neg")) %>%
    mutate(stock_cd_f = factor(stock_cd_f, levels = plot_df %>% select(stock_cd_f, pred_mean) %>% unique() %>% arrange(desc(pred_mean)) %>% pull(stock_cd_f))) %>% 
    ggplot(aes(order, value, fill = shap_sign)) +
    geom_bar(stat = "identity", show.legend = FALSE, alpha=0.5) +
    facet_wrap(~ stock_cd_f, scales="free_y") +
    theme_minimal(base_family='NanumGothic') +
    coord_flip() +
    geom_text(
      aes(x=order, y=0, label=paste0(variable," : [",round(rfvalue, 2), "]")), size=3
    ) +
    scale_fill_manual(values=c("#E41A1C", "#377EB8")) +
    theme(axis.text.y = element_blank(),
          axis.title.x = element_blank(),
          axis.title.y = element_blank())
}
