
# Prepare Data for Modeling

#' @export
prep_data = function(df, beg_date = '2011-01-01'){

  formatted = df %>%
    as.data.frame() %>%
    mutate(date = ymd(date)) %>%
    mutate(stock_cd = str_pad(stock_cd, 6,side = c('left'), pad = '0')) %>%
    filter(date >= as.Date(beg_date)) %>% 
    as.data.frame()

  return(formatted)

}

#' @export
get_modeling_data = function(period_gb = "Monthly", bizday = 3, extract_start_date = '20211001', min_years_listed = 2) {
  
  extract_start_date <- str_replace_all(extract_start_date, '-', '')
  rank_norm = function(x){
    (rank(x, na.last="keep") - min(rank(x, na.last="keep"), na.rm=T)) / 
      (max(rank(x, na.last="keep"), na.rm=T) - min(rank(x, na.last="keep"), na.rm=T))
  }
  
  if (!period_gb %in% c("Monthly", "Bi-Weekly", "Weekly")) stop ("period_gb must be one of 'Monthly', 'Bi-Weekly', 'Weekly'")
  if (period_gb == 'Monthly' & is.numeric(bizday) == FALSE) stop ("bizday must be integer between 1 and 20, if period_gb is chosen as 'Monthly'.")
  
  conn = dbConnect(MySQL(),
                   user = 'betterlife',
                   password = 'snail132',
                   host = 'betterlife.duckdns.org',
                   port = 1231 ,
                   dbname = 'stock_db')
  dbSendQuery(conn, "SET NAMES utf8;")
  dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
  dbSendQuery(conn, "SET character_set_connection=utf8mb4;")
  
  if (period_gb == 'Monthly') {
    dates_needed <- dbGetQuery(conn, paste0("select * from stock_db.d_working_day_rank_by_month where date >= ", extract_start_date," and `rank` = ", bizday))$date
  } else if (period_gb == 'Bi-Weekly') {
    dates_needed <- dbGetQuery(conn, paste0("select * from stock_db.d_working_day where date >= ", extract_start_date," and seq%10 = 0"))$date
  } else if (period_gb == 'Weekly') {
    dates_needed <- dbGetQuery(conn, paste0("select * from stock_db.d_working_day where date >= ", extract_start_date," and seq%5 = 0"))$date
  }
  
  factor_df <- dbGetQuery(conn, paste0("select * from stock_db.d_final_factor where date in (", paste0(dates_needed, collapse = ','), ")"))
  target_df <- dbGetQuery(conn, paste0("select * from stock_db.d_final_target where date in (", paste0(dates_needed, collapse = ','), ")"))
  macro_df <- dbGetQuery(conn, paste0("select * from stock_db.d_final_macro where date in (", paste0(dates_needed, collapse = ','), ")"))
  
  step0 <- 
    factor_df %>% 
    left_join(macro_df, by = "date") %>% 
    left_join(target_df, by = c("date", "stock_cd"))
  rm(factor_df); rm(target_df); rm(macro_df)
  
  # 1. 재무 NA 비율 30% 이상 제거 === 
  step1 <-
    step0 %>% 
    mutate(fs_na_ratio = step0 %>% select(ends_with("ttm")) %>% select(-contains("leverage")) %>% is.na() %>% rowMeans()) %>% 
    filter(fs_na_ratio < 0.3) %>% 
    select(-fs_na_ratio)
  
  # 2. 금융주 제거 ===
  fin_stock_cd <- 
    dbGetQuery(conn, paste0("select date, stock_cd, stock_nm from stock_db.stock_market_sector where date in (", paste0(dates_needed, collapse=','),") and stock_nm rlike '(은행|카드|증권|보험|코리안리|미래에셋대우|자산관리|금융|글로벌텍스프리|창투|인베스트|모기지|CNH|우리파이낸셜|캐피탈|화재|해상|신한지주|신한알파리츠|삼성생명|동양생명|한화생명|미래에셋생명|아이엔지생명|투자|종금|스팩|에이플러스에셋|[0-9]호|한국토지신탁)'"))
  step2 <-
    step1 %>% 
    left_join(fin_stock_cd, by=c("date", "stock_cd")) %>% 
    filter(is.na(stock_nm)) %>% 
    select(-stock_nm)
  rm(fin_stock_cd)
  
  # 3. 상장 후 N년 ===
  days_listed <- dbGetQuery(conn, paste0("select date, stock_cd, date_from_ipo from stock_db.stock_derived_var where date in (", paste0(dates_needed, collapse=','),")"))
  step3 <-
    step2 %>% 
    left_join(days_listed, by=c("date", "stock_cd")) %>% 
    filter(date_from_ipo/365 >= min_years_listed) %>% 
    select(-date_from_ipo)
  rm(days_listed)
  
  # 4. Rank-Normalize ===
  step4 <-
    step3 %>% 
    group_by(date) %>%
    mutate_at(vars(ends_with("ttm")), function(x){rank_norm(x)}) %>%
    mutate_at(c('o_cfr', 'o_per', 'pbr', 'per', 'psr'), function(x){rank_norm(-1/x)}) %>%
    mutate(tr = rank_norm(tr),
           transaction_amount_1w_mean = rank_norm(transaction_amount_1w_mean),
           market_capitalization = rank_norm(market_capitalization)) %>%
    ungroup()
  
  # 5. Remove Redundant Factors ===
  step5 <-
    step4 %>% 
    select(-starts_with(c('cfo_ttm', 'cfi_ttm', 'cff_ttm', 'turnover_ttm', 'roe_ttm', 'roa_ttm', 'roc_ttm', 'op_margin_ttm'))) %>% 
    select(-starts_with(c('call_interest_rate', 'cd_interest_rate', 'kor_interest_rate_3y', 'kor_interest_rate_5y', 'us_interest_rate_6m', 'us_interest_rate_3y', 'us_interest_rate_30y')))
  
  # Prep Data ===
  result = 
    step5 %>% 
    arrange(date, stock_cd) %>% 
    mutate(date = ymd(date)) %>%
    as.data.frame()
  
  dbDisconnect(conn)
  return(result)
}
