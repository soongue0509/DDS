# Util Funcs

# ssl_join

#' @export
ssl_mix <- function(ssl1, ssl2, ssl1_ratio) {
  result <-
    dplyr::inner_join(
      ssl1 %>% select(date, stock_cd, ssl1 = pred_mean, target_1m_return),
      ssl2 %>% select(date, stock_cd, ssl2 = pred_mean),
      by=c("date", "stock_cd")
    ) %>%
    mutate(pred_mix = ssl1*ssl1_ratio + ssl2*(1-ssl1_ratio)) %>%
    select(date, stock_cd, ssl1, ssl2, pred_mix, target_1m_return) %>%
    arrange(date) %>%
    group_by(date) %>%
    arrange(desc(pred_mix), .by_group = T) %>%
    ungroup()
  return(result)
}

#' @export
ssl_intersect <- function(ssl1, ssl2, topN) {
  result = 
    inner_join(ssl1 %>% group_by(date) %>% top_n(topN, pred_mean) %>% select(date, stock_cd, ssl1 = pred_mean, target_1m_return),
               ssl2 %>% group_by(date) %>% top_n(topN, pred_mean) %>% select(date, stock_cd, ssl2 = pred_mean),
               by=c("date", "stock_cd")) %>% 
    ungroup() %>% 
    select(date, stock_cd, ssl1, ssl2, target_1m_return)
  return(result)
}

#' @export
ssl_bind <- function(ssl1, ssl2, topN1, topN2) {
  result <-
    rbind(
      ssl1 %>% select(date, stock_cd, pred_mean, target_1m_return) %>% group_by(date) %>% arrange(desc(pred_mean)) %>% dplyr::slice(1:topN1) %>% ungroup() %>% mutate(gubun = 'ssl1'),
      ssl2 %>% select(date, stock_cd, pred_mean, target_1m_return) %>% group_by(date) %>% arrange(desc(pred_mean)) %>% dplyr::slice(1:topN2) %>% ungroup() %>% mutate(gubun = 'ssl2')
    ) %>% 
    group_by(date, stock_cd) %>% 
    summarize(pred_mean = mean(pred_mean),
              target_1m_return = unique(target_1m_return),
              gubun = ifelse(n() == 2, 'both', gubun)) %>% 
    ungroup() %>% 
    arrange(date, stock_cd)
  return(result)
}

#' @export
sector_neutral <- function(ssl, SN_ratio, topN, pred_col) {
  library(RMySQL)
  conn <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db'
  )
  dbSendQuery(conn, "SET NAMES utf8;")
  dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
  dbSendQuery(conn, "SET character_set_connection=utf8mb4;")
  
  # Sector
  sector_info <- dbGetQuery(conn, paste0("select * from stock_market_sector where date in (", paste0(str_replace_all(unique(ssl$date),'-',''), collapse = ','), ")")) %>% prep_data()
  Encoding(sector_info$stock_nm) = 'UTF-8'; Encoding(sector_info$sector) = 'UTF-8'
  
  # Sector Neutral =====
  max_stock_per_sector = floor(topN*SN_ratio)
  # Create Sector Neutral SSL
  ssl_sn <-
    ssl %>%
    left_join(sector_info %>% select(date, stock_cd, stock_nm, market, sector), by=c("date", "stock_cd")) %>%
    group_by(date, sector) %>%
    arrange(desc(get(pred_col)), .by_group =T) %>%
    dplyr::slice(1:max_stock_per_sector) %>%
    group_by(date) %>%
    arrange(desc(get(pred_col)), .by_group =T) %>%
    ungroup()
  
  # Disconnect MySQL Server
  dbDisconnect(conn)
  
  return(ssl_sn)
}

#' @export
ta_filtering <- function(ssl, min_transaction_amount = 1e8) {
  library(RMySQL)
  conn <- dbConnect(MySQL(),
                    user = 'betterlife',
                    password = 'snail132',
                    host = 'betterlife.duckdns.org',
                    port = 1231,
                    dbname = 'stock_db')
    
  ta1w <- dbGetQuery(conn, paste0("
                      select date, stock_cd, transaction_amount_1w_mean 
                      from stock_db.d_final_factor 
                      where date in (", paste0(str_replace_all(unique(ssl$date),'-',''), collapse = ','), ")"))
    
  ssl_filtered <- 
    ssl %>% 
    left_join(ta1w %>% prep_data(), by=c("date", "stock_cd")) %>% 
    filter(transaction_amount_1w_mean >= min_transaction_amount) %>% 
    select(-transaction_amount_1w_mean)
  
  dbDisconnect(conn)
  
  return(ssl_filtered)
}

#' @export
exclude_issue_func <- function(ssl) {
  library(RMySQL)
  conn <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db')
  
  issue_df <- dbGetQuery(conn, paste0("select * from stock_db.stock_issue where issue = 1 and date in (", paste0(str_replace_all(unique(ssl$date),'-',''), collapse = ','), ")")) %>% prep_data()
  
  ssl_issue_excluded <-
    ssl %>% 
    left_join(issue_df, by=c("date", "stock_cd")) %>% 
    filter(is.na(issue)) %>% 
    select(-issue)
  
  dbDisconnect(conn)
  
  return(ssl_issue_excluded)
}

#' @export
auc_calc = function(ssl, target_y, by_month=FALSE) {
  library(RMySQL)
  conn <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db')
  target_df <- dbGetQuery(conn, paste0("select date, stock_cd, ", target_y, " from stock_db.d_final_target where date in (", paste0(str_replace_all(unique(ssl$date),'-',''), collapse = ','), ")")) %>% prep_data()
  dbDisconnect(conn)
  
  auc_df =
    ssl %>% 
    filter(date != max(date)) %>% 
    select(date, stock_cd, pred_mean) %>% 
    left_join(target_df %>% select(date, stock_cd, target_y), by=c("date", "stock_cd")) %>% 
    mutate(response = get(target_y)) %>% 
    group_by(date) %>% 
    summarize(AUC = Metrics::auc(response, pred_mean))
  
  if (by_month) {
    return(auc_df)
  } else {
    return(mean(auc_df$AUC))
  }
}

#' @export
topN_prec_calc = function(ssl, target_y, topN, by_month=FALSE) {
  library(RMySQL)
  conn <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db')
  target_df <- dbGetQuery(conn, paste0("select date, stock_cd, ", target_y, " from stock_db.d_final_target where date in (", paste0(str_replace_all(unique(ssl$date),'-',''), collapse = ','), ")")) %>% prep_data()
  dbDisconnect(conn)
  
  prec_df =
    ssl %>% 
    filter(date != max(date)) %>% 
    select(date, stock_cd, pred_mean) %>% 
    left_join(target_df %>% select(date, stock_cd, target_y), by=c("date", "stock_cd")) %>% 
    mutate(response = get(target_y)) %>% 
    group_by(date) %>% 
    arrange(desc(pred_mean), .by_group=T) %>% 
    dplyr::slice(1:topN) %>% 
    summarize(Precision = sum(response)/topN)

  if (by_month) {
    return(prec_df)
  } else {
    return(mean(prec_df$Precision))
  }
}

#' @export
load_price_data = function(start_date = '20150101') {
  
  start_date <- str_replace_all(start_date, '-', '')
  
  library(RMySQL)
  conn <- dbConnect(
    MySQL(),
    user = 'betterlife',
    password = 'snail132',
    host = 'betterlife.duckdns.org',
    port = 1231,
    dbname = 'stock_db'
  )
  dbSendQuery(conn, "SET NAMES utf8;")
  dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
  dbSendQuery(conn, "SET character_set_connection=utf8mb4;")
  
  # Stock Price
  d_stock_price <<- dbGetQuery(conn, paste0("select * from stock_adj_price where date >= '", start_date ,"';"))
  # KOSPI & KOSDAQ
  d_kospi_kosdaq <<- dbGetQuery(conn, paste0("select date, kospi, kosdaq from stock_kospi_kosdaq where date >= '", start_date, "';"))
  # Safe Haven
  safe_haven_price <<- dbGetQuery(conn, "select * from stock_db.stock_adj_price where stock_cd = '261240'")
  
  # Disconnect MySQL Server
  dbDisconnect(conn)
}

#' @export
upper_bound_calc = function(ssl, top_n, first_bound=0.50, second_plus=0.30, num_tries, load_price_data = TRUE) {
  
  if (num_tries <= 0) {
    stop("num_tries must be greater than 0")
  }
  if (first_bound < 0.1) {
    stop("first_bound must be greater than 0.1")
  }
  if (second_plus < 0.05) {
    stop("second_plus must be greater than 0.05")
  }
  
  if (load_price_data) {
    library(RMySQL)
    conn <- dbConnect(
      MySQL(),
      user = 'betterlife',
      password = 'snail132',
      host = 'betterlife.duckdns.org',
      port = 1231 ,
      dbname = 'stock_db'
    )
    dbSendQuery(conn, "SET NAMES utf8;")
    dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
    dbSendQuery(conn, "SET character_set_connection=utf8mb4;")
    d_stock_price <-
      dbGetQuery(conn, "select * from stock_adj_price where date >= '20170101';") %>% 
      mutate(date = ymd(date))
    dbDisconnect(conn)
  }
  
  first_upper_bound_vec = seq(0.1, first_bound, by=0.05)
  second_upper_plus_vec = seq(0.0, second_plus, by=0.05)
  bound_cumret_df <- data.frame()
  
  for (j in 1:num_tries) {
    
    print(j)
    
    set.seed(j)
    ssl_sample <- 
      ssl %>% 
      mutate(pred_sample_mean = rowMeans(ssl %>% select(sample(colnames(ssl)[str_detect(colnames(ssl), 'pred\\d')], 50, replace=T)))) %>% 
      select(date,stock_cd, pred_sample_mean) %>% 
      group_by(date) %>% 
      arrange(desc(pred_sample_mean), .by_group=TRUE)
    
    rebalancing_dates = sort(unique(ssl_sample$date))
    
    for (first_upper_bound in first_upper_bound_vec) {
      for (second_upper_plus in second_upper_plus_vec) {
        
        second_upper_bound = first_upper_bound + second_upper_plus
        
        print(paste0("first_upper: ", first_upper_bound, "  /  second_upper: ", second_upper_bound))
        
        for(k in rebalancing_dates[-length(rebalancing_dates)]) {
          i = as.Date(k, origin = '1970-01-01')
          
          portfolio_return <-
            d_stock_price %>%
            select(-adj_open_price) %>% 
            filter(date >= i) %>%
            filter(date <= rebalancing_dates[which(rebalancing_dates==i)+1]) %>%
            filter(stock_cd %in% (ssl_sample %>% filter(date == i) %>% arrange(desc(pred_sample_mean)) %>% dplyr::slice(1:top_n) %>% pull(stock_cd))) %>% 
            group_by(stock_cd) %>% 
            mutate(adj_low_price = ifelse(adj_low_price == 0, adj_close_price, adj_low_price),
                   adj_high_price = ifelse(adj_high_price == 0, adj_close_price, adj_high_price)) %>% 
            mutate(start_price = adj_close_price[1]) %>%
            mutate(first_upper_price = start_price * (1+first_upper_bound), second_upper_price = start_price * (1+second_upper_bound)) %>% 
            mutate(first_upper_yn = ifelse(adj_high_price >= first_upper_price, 1, NA),
                   second_upper_yn = ifelse(adj_high_price >= second_upper_price, 1, NA)) %>% 
            mutate(first_upper_yn = ifelse(first_upper_yn * (which.min(is.na(first_upper_yn)) == row_number()) == 1, 1, NA),
                   second_upper_yn = ifelse(second_upper_yn * (which.min(is.na(second_upper_yn)) == row_number()) == 1, 1, NA)) %>% 
            mutate(temp = case_when(first_upper_yn == 1 ~ "first_upper",
                                    second_upper_yn == 1 ~ "second_upper")) %>% 
            mutate(d_return = (adj_close_price - lag(adj_close_price))/lag(adj_close_price)) %>%
            filter(!is.na(d_return)) %>%
            mutate(cum_return_temp = cumprod(d_return+1) - 1) %>%
            filter(!is.na(temp) | date == max(date)) %>%
            bind_rows(., .[,] %>% filter(first_upper_yn == 1 & second_upper_yn == 1) %>% mutate(temp = "second_upper")) %>%
            arrange(stock_cd, date, .by_group=T) %>%
            dplyr::slice(1:2) %>%
            mutate(wt = 1/n()) %>%
            mutate(cum_return = case_when(temp == "first_upper" ~ first_upper_bound,
                                          temp == "second_upper" ~ second_upper_bound,
                                          TRUE ~ cum_return_temp)) %>%
            ungroup() %>% 
            summarize(portfolio_return = weighted.mean(cum_return,wt),
                      first_cnt = sum(ifelse(temp == 'first_upper', 1, 0), na.rm=T), 
                      second_cnt = sum(ifelse(temp == 'second_upper', 1, 0), na.rm=T))
          
          bound_cumret_df <- rbind(bound_cumret_df, data.frame(try_idx = j, date = i, first_upper_bound = first_upper_bound, second_upper_bound = second_upper_bound, 
                                                               portfolio_return = portfolio_return$portfolio_return, 
                                                               first_cnt = portfolio_return$first_cnt,
                                                               second_cnt = portfolio_return$second_cnt))
          
        }
      }
    }
  }
  
  gp <-
    bound_cumret_df %>%
    filter(substr(as.Date(date, origin ='1970-01-01'), 1, 7) != c('2020-03')) %>% 
    filter(substr(as.Date(date, origin ='1970-01-01'), 1, 7) != c('2020-04')) %>% 
    group_by(try_idx, first_upper_bound, second_upper_bound) %>% 
    mutate(cumret = cumprod(portfolio_return+1)-1, 
           first_cnt = sum(first_cnt), 
           second_cnt = sum(second_cnt)) %>% 
    filter(date == max(date)) %>% 
    ungroup() %>% 
    mutate(cnt = ifelse(first_upper_bound == second_upper_bound, as.character(first_cnt), paste0(first_cnt, '→', second_cnt))) %>% 
    select(try_idx, first_upper_bound, second_upper_bound, cumret, cnt) %>% 
    as.data.frame() %>% 
    ggplot(aes(x=first_upper_bound, y=second_upper_bound, fill=cumret, label=cnt)) + 
    geom_tile(width=0.05,height=0.05) +
    geom_text() +
    facet_wrap(try_idx ~ .) +
    theme_minimal()
  print(gp)
  
  options(dplyr.summarise.inform = FALSE)
  max_hit_ratio_df =
    bound_cumret_df %>%
    group_by(try_idx, first_upper_bound, second_upper_bound) %>% 
    summarize(hit_ratio = sum(ifelse(portfolio_return > 0, 1, 0))/n()) %>% 
    group_by(first_upper_bound, second_upper_bound) %>% 
    summarize(hit_ratio = mean(hit_ratio))
  
  print(paste0("[Hit Ratio] upper : ", max_hit_ratio_df$first_upper_bound, " / lower : ", max_hit_ratio_df$second_upper_bound, " / hit ratio : ", round(max_hit_ratio_df$hit_ratio, 4)))
}

#' @export
upper_hoga_calc = 
  function(market_gubun, my_price, upper_bound) {
    if(market_gubun == 'KOSPI') {
      if(my_price*upper_bound < 1000) {
        sell_price = as.integer(ceiling(my_price*upper_bound))
      } else if (my_price*upper_bound >= 1000 & my_price*upper_bound < 5000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 5) * 5)
      } else if (my_price*upper_bound >= 5000 & my_price*upper_bound < 10000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 10) * 10)
      } else if (my_price*upper_bound >= 10000 & my_price*upper_bound < 50000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 50) * 50)
      } else if (my_price*upper_bound >= 50000 & my_price*upper_bound < 100000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 100) * 100)
      } else if (my_price*upper_bound >= 100000 & my_price*upper_bound < 500000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 500) * 500)
      } else {
        sell_price = as.integer(ceiling(my_price*upper_bound / 1000) * 1000)
      }
    }
    else {
      if (my_price*upper_bound < 1000) {
        sell_price = as.integer(ceiling(my_price*upper_bound))
      } else if (my_price*upper_bound >= 1000 & my_price*upper_bound < 5000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 5) * 5)
      } else if (my_price*upper_bound >= 5000 & my_price*upper_bound < 10000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 10) * 10)
      } else if (my_price*upper_bound >= 10000 & my_price*upper_bound < 50000){
        sell_price = as.integer(ceiling(my_price*upper_bound / 50) * 50)
      } else {
        sell_price = as.integer(ceiling(my_price*upper_bound / 100) * 100)
      }
    }
    return(sell_price)
  }
