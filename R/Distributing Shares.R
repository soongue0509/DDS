# Distributing Shares

# How many shares

#' @export
how_many_shares = function(ssl, inv_date, seed_money, pred_col, topN=30, view_method="long", SN_ratio=0.3) {

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

  print(paste0("SSL Date: ", str_replace_all(ssl %>% filter(date == max(date)) %>% pull(date) %>% unique(), '-', '')))
  print(paste0("Inv Date: ", inv_date))

  # Read Data
  sector_info = dbGetQuery(stock_db_connection, paste0("select * from stock_market_sector where date = '", inv_date,"';"))
  Encoding(sector_info$stock_nm) = 'UTF-8'; Encoding(sector_info$sector) = 'UTF-8'

  d_stock_price_temp <-
    dbGetQuery(stock_db_connection, paste0("select * from stock_adj_price where date = '", inv_date,"';")) %>%
    select(stock_cd, date, price = adj_close_price)

  ssl_temp =
    ssl %>%
    filter(date == max(date)) %>%
    left_join(sector_info %>% select(stock_cd, stock_nm, sector), by="stock_cd") %>%
    group_by(sector) %>%
    arrange(desc(get(pred_col)), .by_group=T) %>%
    dplyr::slice(1:floor(topN*SN_ratio)) %>%
    ungroup() %>%
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
    select(stock_cd, stock_nm, all_of(pred_col), cnt=cnt_temp, amt, proportion, last_close_price=price)

  print("============================================================================")
  print(paste0("Surplus: ", surplus))

  if (view_method == "long") {
    View(final_result)
    return(final_result)
  } else {
    View(t(final_result))
    return(t(final_result))
  }
}

# SHAP for chosen stocks

#' @export
explain_why = function(shap1, ssl1, shap2, ssl2, join_ratio, top_N, inv_date) {
  
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
  
  stock_nm = dbGetQuery(stock_db_connection, paste0("select stock_cd, stock_nm from stock_market_sector where date = '", inv_date,"';"))
  
  temp1 <-
    left_join(
      shap1 %>% select(date, stock_cd, variable, value, rfvalue),
      ssl1 %>% select(date, stock_cd, pred_mean),
      by=c("date", "stock_cd")
    )
  temp2 <-
    left_join(
      shap2 %>% select(date, stock_cd, variable, value, rfvalue),
      ssl2 %>% select(date, stock_cd, pred_mean),
      by=c("date", "stock_cd")
    )
  
  temp <- 
    left_join(temp1, temp2, by=c("date", "stock_cd", "variable")) %>% 
    mutate(value = value.x*join_ratio + value.y*(1-join_ratio),
           rfvalue = rfvalue.x*join_ratio + rfvalue.y*(1-join_ratio),
           pred_mean = pred_mean.x*join_ratio + pred_mean.y*(1-join_ratio)) %>% 
    select(date, stock_cd, variable, value, rfvalue, pred_mean)
  
  plot_df <-
    temp %>% 
    inner_join(
      temp %>% 
        select(date, stock_cd, pred_mean) %>% 
        unique() %>% 
        arrange(desc(pred_mean)) %>% 
        dplyr::slice(1:top_N) %>% 
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
