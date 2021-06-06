# Distributing Shares

# How many shares

#' @export
how_many_shares = function(seed_money, ssl, pred_col, topN, inv_date, view_method, SN_ratio) {

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
  } else {
    View(t(final_result))
  }
}
