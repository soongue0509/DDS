# Backtesting Module

# Initial settings including loading data from DB and SSL from local/cloud

#' @export
modeling_func = function(df, target_y, title = "", num_threads_params=12, train_span=36, push_span=1, ensemble_n = 300, bagging_prop=0.8, feature_prop=0.8, num_rounds=60, pred_start_date = '2015-01-01', explain_yn = 'N', focal_loss_yn = 'N', load_to_db = FALSE) {
  
  pred_start_date = ymd(pred_start_date)
  if(!is.logical(load_to_db)) stop("load_to_db must be logical")
  
  sigmoid = function(x) {
    1 / (1 + exp(-x))
  }
  
  focal_loss_lgb <- function(preds, dtrain) {
    y_true <- getinfo(dtrain, "label")
    p <- 1 / (1+exp(-preds))
    grad <- 2 * ((y_true - (1 - y_true)) * (1 - (y_true * p + (1 - y_true) * (1 - p)))) * (y_true * log(p) + (1 - y_true) * log(1 - p)) - ((1 - (y_true * p + (1 - y_true) * (1 - p)))^2) * (y_true * (1/p) - (1 - y_true) * (1/(1 - p)))
    hess <- 2 * ((y_true - (1 - y_true)) * (1 - (y_true * p + (1 - y_true) * (1 - p)))) *
      (y_true * (1/p) - (1 - y_true) * (1/(1 - p))) -
      2 * ((y_true - (1 - y_true)) * (y_true - (1 - y_true))) *
      (y_true * log(p) + (1 - y_true) * log(1 - p)) +
      (((1 - (y_true * p + (1 - y_true) * (1 - p)))^2) * (y_true * (1/p^2) + (1 - y_true) * (1/(1 - p)^2)) + 2 * ((y_true - (1 - y_true)) * (1 - (y_true * p + (1 - y_true) * (1 - p)))) * (y_true * (1/p) - (1 - y_true) * (1/(1 - p))))
    return(list(grad = grad, hess = hess))
  }
  
  shap.values <- function(xgb_model,
                          X_train){
    
    shap_contrib <- predict(xgb_model,
                            (X_train),
                            predcontrib = TRUE)
    
    # Add colnames if not already there (required for LightGBM)
    if (is.null(colnames(shap_contrib))) {
      colnames(shap_contrib) <- c(colnames(X_train), "BIAS")
    }
    
    shap_contrib <- as.data.table(shap_contrib)
    
    # For both XGBoost and LightGBM, the baseline value is kept in the last column
    BIAS0 <- shap_contrib[, ncol(shap_contrib), with = FALSE][1]
    
    # Remove baseline and ensure the shap matrix has column names
    shap_contrib[, `:=`(BIAS, NULL)]
    
    # Make SHAP score in decreasing order
    imp <- colMeans(abs(shap_contrib))
    mean_shap_score <- imp[order(imp, decreasing = T)]
    
    return(list(shap_score = shap_contrib,
                mean_shap_score = mean_shap_score,
                BIAS0 = BIAS0))
  }
  
  shap.prep <- function(xgb_model = NULL,
                        shap_contrib = NULL, # optional to directly supply SHAP values
                        X_train,
                        top_n = NULL,
                        var_cat = NULL
  ){
    if (is.null(xgb_model) & is.null(shap_contrib)) stop("Please provide either `xgb_model` or `shap_contrib`")
    if (!is.null(shap_contrib)){
      if(paste0(dim(shap_contrib), collapse = " ") != paste0(dim(X_train), collapse = " ")) stop("supply correct shap_contrib, remove BIAS column.\n")
    }
    
    # prep long-data
    shap <- if (is.null(shap_contrib)) shap.values(xgb_model, X_train) else list(
      shap_score = shap_contrib,
      mean_shap_score = colMeans(abs(shap_contrib))[order(colMeans(abs(shap_contrib)), decreasing = TRUE)]
    )
    std1 <- function(x){
      return ((x - min(x, na.rm = TRUE))/(max(x, na.rm = TRUE) - min(x, na.rm = TRUE)))
    }
    
    # choose top n features
    if (is.null(top_n)) top_n <- dim(X_train)[2] # by default, use all features
    top_n <- as.integer(top_n)
    if (!top_n%in%c(1:dim(X_train)[2])) {
      message ('Please supply correct top_n, by default use all features.\n')
      top_n <- dim(X_train)[2]
    }
    
    # arrange variables in descending order, thus the summary plot could be
    # plotted accordingly.
    shap_score_sub <- setDT(shap$shap_score)[, names(shap$mean_shap_score)[1:top_n], with = FALSE]
    shap_score_sub[, ID:= .I]
    # fv: feature values: the values in the original dataset
    # fv_sub: subset of feature values
    # since dayint is int, the package example will throw a warning here
    fv_sub <- as.data.table(X_train)[, names(shap$mean_shap_score)[1:top_n], with = F]
    
    if(is.null(var_cat)){
      # shap_score_sub contains the sample ID
      shap_score_long <- melt.data.table(shap_score_sub, measure.vars = colnames(fv_sub))
      vars_wanted <- colnames(fv_sub)
      
    } else if (var_cat%in%colnames(fv_sub)) {
      # exclude var_cat as it is used as a categorical group
      shap_score_long <- melt.data.table(shap_score_sub[,-..var_cat], measure.vars = colnames(fv_sub)[!colnames(fv_sub) %in% c(var_cat, "ID")])
      vars_wanted <- colnames(fv_sub)[!colnames(fv_sub) %in% var_cat]
    } else {
      stop("Please provide a correct var_cat variable, a categorical variable that
         exists in the dataset.")
    }
    # standardize feature values
    fv_sub_long <- melt.data.table(fv_sub, measure.vars = vars_wanted, value.name = "rfvalue")
    fv_sub_long[, stdfvalue := std1(rfvalue), by = "variable"]
    # SHAP value: value
    # raw feature value: rfvalue;
    # standarized: stdfvalue
    if(is.null(var_cat)){
      shap_long2 <- cbind(shap_score_long, fv_sub_long[,c('rfvalue','stdfvalue')])
    } else {
      shap_long2 <- cbind(shap_score_long, fv_sub_long[,c('rfvalue','stdfvalue', var_cat), with = FALSE])
    }
    # mean_value: mean abs SHAP values by variable, used as the label by
    # `geom_text` in the summary plot
    shap_long2[, mean_value := mean(abs(value)), by = variable]
    setkey(shap_long2, variable)
    return(shap_long2)
  }
  
  selected_stock_cd_list = list()
  shap_train_df <- data.frame()
  shap_test_df <- data.frame()
  k = 1
  
  # Set Data Span
  pre_bt <-
    df %>%
    filter(date < pred_start_date)
  post_bt <-
    df %>%
    filter(date >= pred_start_date)
  pre_bt_date <- unique(pre_bt$date)
  data_bt <-
    rbind(pre_bt %>% filter(between(date, pre_bt_date[length(pre_bt_date)-train_span+1], pre_bt_date[length(pre_bt_date)])),
          post_bt)
  date_unique_bt <- unique(data_bt$date)
  
  # Set Training Parameters
  lgbm_params <-
    list(
      learning_rate = 0.1,
      bagging_fraction = bagging_prop,
      feature_fraction = feature_prop,
      bagging_freq = 1
    )
  
  for (i in seq(1, by=push_span, to = length(date_unique_bt) - train_span)) {
    tic()
    
    # Make Train & Validation Set
    dt_train <-
      data_bt %>%
      # filter(date <= date_unique_bt[i+train_span-1]) # 롤링 오리진
      filter(between(date, date_unique_bt[i], date_unique_bt[i+train_span-1]))
    dt_test <-
      data_bt %>%
      filter(between(date, date_unique_bt[i+train_span], coalesce(date_unique_bt[i+train_span+push_span-1], max(date_unique_bt))))
    
    if(nrow(dt_test) == 0) break
    
    train_y <- dt_train[,target_y]
    
    lgbm_train_dat <-
      lgb.Dataset(
        data.matrix(dt_train %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))),
        label = dt_train[,target_y]
      )
    lgbm_test_dat <- data.matrix(dt_test %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return'))))
    
    # Start Training
    pred_mat <- matrix(nrow=nrow(dt_test), ncol=ensemble_n)
    
    if(focal_loss_yn == 'Y') {
      for (m in 1:ensemble_n) {
        
        lgbm_model <-
          lightgbm(
            params = lgbm_params,
            data = lgbm_train_dat,
            boosting = "gbdt",
            nrounds = num_rounds,
            num_threads = num_threads_params,
            tree_learner = "voting",
            verbose = -1,
            seed = m,
            objective = focal_loss_lgb
          )
        
        # Predict
        lgbm_pred <- sigmoid(predict(lgbm_model, lgbm_test_dat))
        pred_mat[,m] <- lgbm_pred
        
      }
    } else {
      for (m in 1:ensemble_n) {
        
        lgbm_model <-
          lightgbm(
            params = lgbm_params,
            data = lgbm_train_dat,
            boosting = "gbdt",
            nrounds = num_rounds,
            num_threads = num_threads_params,
            tree_learner = "voting",
            verbose = -1,
            seed = m,
            objective = "binary"
          )
        
        # Predict
        lgbm_pred <- predict(lgbm_model, lgbm_test_dat)
        pred_mat[,m] <- lgbm_pred
        
      }
    }
    
    colnames(pred_mat) <- paste0('pred', seq(1:ensemble_n))
    pred_mean <- apply(pred_mat, 1, mean)
    Valid_Data = cbind(dt_test, pred_mat, pred_mean)
    top_list <-
      Valid_Data %>%
      group_by(date) %>%
      arrange(desc(pred_mean), .by_group=T) %>%
      select(date, stock_cd, contains("pred")) %>%
      ungroup()
    
    if(explain_yn == 'Y'){
      
      # SHAP
      shap_train <- shap.prep(xgb_model = lgbm_model, X_train = data.matrix(dt_train %>% sample_n(1000) %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))))
      shap_test <- shap.prep(xgb_model = lgbm_model, X_train = data.matrix(dt_test %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))))
      
      shap_train_df <-
        rbind(
          shap_train_df,
          shap_train %>%
            mutate(date = unique(dt_test$date)) %>%
            select(date, variable, value, rfvalue, stdfvalue, mean_value)
        )
      
      shap_test_df <-
        rbind(
          shap_test_df,
          shap_test %>%
            left_join(
              cbind(data.frame(ID=sort(unique(shap_test$ID))), dt_test %>% select(date, stock_cd)),
              by="ID"
            ) %>%
            select(date, stock_cd, variable, value, rfvalue, stdfvalue, mean_value)
        )
      
    }
    
    # saving on 'selected_stock_cd_list'
    selected_stock_cd_list[[k]] <- top_list
    
    k = k+1
    
    print(top_list$date %>% unique())
    toc()
  }
  ssl <<- data.frame()
  for (i in 1:length(selected_stock_cd_list)) {
    ssl <<- rbind(ssl, selected_stock_cd_list[[i]])
  }
  ssl %<>% left_join(df %>% select(date, stock_cd, target_1m_return), by = c('date', 'stock_cd'))
  saveRDS(ssl, paste0("ssl_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y," (n", ensemble_n, ").RDS"))
  if(explain_yn == 'Y'){
    saveRDS(shap_train_df, paste0("trainSHAP_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y, ".RDS"))
    saveRDS(shap_test_df %>% left_join(ssl %>% select(date, stock_cd, pred_mean), by=c("date", "stock_cd")), paste0("testSHAP_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y, ".RDS"))
  }
  if(load_to_db){
    conn <- dbConnect(
      MySQL(),
      user = 'betterlife',
      password = 'snail132',
      host = 'betterlife.duckdns.org',
      port = 1231 ,
      dbname = 'ssl_db')
    dbSendQuery(conn, "SET NAMES utf8;") 
    dbSendQuery(conn, "SET CHARACTER SET utf8mb4;") 
    dbSendQuery(conn, "SET character_set_connection=utf8mb4;")
    
    dbWriteTable(conn,
                 name = paste0("ssl_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y,"_n", ensemble_n),
                 value = ssl %>% select(date, stock_cd, pred_mean) %>% as.data.frame(),
                 row.names = FALSE,
                 overwrite = TRUE,
                 append = FALSE,
                 field.types = field.types = c(date="int", stock_cd="varchar(6)", pred_mean="double")
                 )
    
    lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
  }
  return(ssl)
}

#' @export
backtest_portfolio_tic =
  function(test_title="Portfolio Return", ssl_list, topN, pred_col, SN_ratio, include_issue, upper_bound, lower_bound, safe_haven = NA, weight_list = NA, start_date = '20170101', end_date = '20201230', load_data = 'Y') {
    
    transaction_fee_rate = 0.00315
    start_date = str_replace_all(start_date, '-', '')
    end_date = str_replace_all(end_date, '-', '')
    
    ddsGetQuery <- function(query) {
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
      result = dbGetQuery(conn, query)
      lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
      return(result)
    }
    
    # Check Arugments =====
    if(ymd(start_date) < '2016-01-04') {
      stop("Start Date must be greater than or equal to '20160104'")
    }
    if(ymd(end_date) > '2020-12-30') {
      stop("End Date must be less than or equal to '20201230'")
    }
    if(length(topN) != length(ssl_list)) {
      stop("topN length must be equal to ssl_list length")
    }
    if(length(pred_col) != length(ssl_list)) {
      stop("pred_col length must be equal to ssl_list length")
    }
    if(length(SN_ratio) != length(ssl_list)) {
      stop("SN_ratio length must be equal to ssl_list length. If you don't wanted to use this argument, use 1 instead")
    }
    if(length(upper_bound) != length(ssl_list)) {
      stop("upper_bound length must be equal to ssl_list length")
    }
    if(length(lower_bound) != length(ssl_list)) {
      stop("lower_bound length must be equal to ssl_list length")
    }
    upper_bound[is.na(upper_bound)] = 999999
    lower_bound[is.na(lower_bound)] = -999999
    if(sum(upper_bound <= 0) > 0) {
      stop("Upper selling bound must be greater than Zero")
    }
    if(sum(lower_bound >= 0) > 0) {
      stop("Lower selling lower bound must be less than Zero")
    }
    if(length(safe_haven) != length(ssl_list)) {
      if(length(safe_haven)==1 & is.na(safe_haven)) {
        invisible()
      } else {
        stop("safe_haven length must be equal to ssl_list length. If you don't wanted to use this argument, use single NA instead")
      }
    }
    if(length(weight_list) != length(ssl_list)) {
      if(length(weight_list)==1 & is.na(weight_list)) {
        invisible()
      } else {
        stop("weight_list length must be equal to ssl_list length. If you don't wanted to use this argument, use single NA instead")
      }
    }
    if(length(include_issue) != length(ssl_list)) {
      stop("include_issue length must be equal to ssl_list length. If you don't wanted to use this argument, use N instead")
    }
    
    # Load Data if Needed =====
    if(load_data == 'Y') {
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
      d_stock_price <- dbGetQuery(conn, paste0("select * from stock_adj_price where date >= '", start_date ,"';"))
      # KOSPI & KOSDAQ
      d_kospi_kosdaq <- dbGetQuery(conn, "select date, kospi, kosdaq from stock_kospi_kosdaq where date >= '20100101';")
      # Sector
      sector_info <- dbGetQuery(conn, "select b.* from (select stock_cd, max(date) as date from stock_market_sector group by stock_cd) as a left join stock_market_sector as b on a.stock_cd = b.stock_cd and a.date = b.date;")
      # Gwanli Stocks
      issue_df <- dbGetQuery(conn, "select * from stock_db.stock_issue where issue = 1")
      # Safe Haven
      safe_haven_price <- dbGetQuery(conn, "select * from stock_db.stock_adj_price where stock_cd = '261240'")
      
      # Disconnect MySQL Server
      lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
    }
    
    tic()
    
    # Prepare Data =====
    d_stock_price %<>%
      mutate(date=ymd(date),
             stock_cd = str_pad(stock_cd, 6,side = c('left'), pad = '0')) %>% 
      # 상폐만 0원으로 처리하려면 아래 한 줄 주석처리 : 거래정지는 정지 시점의 종가로 계산
      # mutate(adj_close_price = ifelse(adj_open_price == 0 & adj_high_price == 0 & adj_low_price == 0 & adj_trading_volume == 0 & adj_close_price != 0, NA, adj_close_price)) %>%
      mutate(adj_low_price = ifelse(adj_low_price == 0, adj_close_price, adj_low_price)) %>% 
      mutate(adj_high_price = ifelse(adj_high_price == 0, adj_close_price, adj_high_price)) %>% 
      mutate(adj_open_price = ifelse(adj_open_price == 0, adj_close_price, adj_open_price)) %>% 
      filter(date <= ymd(end_date))
    d_kospi_kosdaq_cum <-
      d_kospi_kosdaq %>%
      mutate(date = ymd(date)) %>% 
      arrange(date) %>%
      mutate(kospi = (kospi-lag(kospi))/lag(kospi),
             kosdaq = (kosdaq - lag(kosdaq))/lag(kosdaq)) %>%
      na.omit() %>% 
      filter(date >= ymd(start_date)) %>%
      filter(date <= ymd(end_date)) %>%
      mutate(kospi_cumret = cumprod(kospi+1)-1, kosdaq_cumret = cumprod(kosdaq+1)-1)
    sector_info %<>% mutate(date=ymd(date))
    issue_df %<>% 
      mutate(date = ymd(date))
    safe_haven_price %<>% 
      select(date, price=adj_close_price) %>% 
      mutate(date = ymd(date))
    
    # Start Simulation =====
    rets_total <- data.frame()
    for (l in 1:length(ssl_list)) {
      # Pre-work =====
      ssl <-
        ssl_list[[l]] %>%
        mutate(date = ymd(date)) %>% 
        filter(date >= ymd(start_date) & date <= ymd(end_date)) %>% 
        group_by(date) %>%
        select(date, stock_cd, pred_col[l]) %>% 
        arrange(desc(get(pred_col[l])), .by_group = TRUE) %>%
        mutate(stock_cd = str_pad(stock_cd, 6,side = c('left'), pad = '0')) %>%
        ungroup()
      
      if (ymd(end_date) == max(ssl$date)) ssl = ssl %>% filter(date != max(ssl$date))
      
      # Remove Gwanli Stocks =====
      if(include_issue[l] == 'N') {
        ssl <- ssl %>% left_join(issue_df %>% unique(), by=c("date", "stock_cd")) %>% filter(is.na(issue)) %>% select(-issue)
      }
      
      # Sector Neutral =====
      max_stock_per_sector = floor(topN[l]*SN_ratio[l])
      # Create Sector Neutral SSL
      sector_filtering_df <-
        ssl %>%
        left_join(sector_info %>% select(stock_cd, sector), by="stock_cd") %>%
        group_by(date, sector) %>%
        arrange(desc(get(pred_col[l])), .by_group =T) %>%
        dplyr::slice(1:max_stock_per_sector) %>% 
        ungroup() %>% 
        select(date, stock_cd, sector)
      ssl_sn <-
        ssl %>%
        inner_join(sector_filtering_df %>% select(-sector), by=c("date", "stock_cd"))
      
      rebalancing_dates <- unique(ssl$date)
      
      rets_cum <- data.frame()
      market_win_vec <- c()
      risk_ratio_vec <- c()
      
      # Work =====
      for(k in rebalancing_dates) {
        i = as.Date(k, origin = '1970-01-01')
        print(i)
        
        # Calculate Each Stock Return =====
        
        # Get Stock Price of Selected Stocks
        # 선택된 종목 틱데이터 호출
        tic_temp = ddsGetQuery(paste0("select date, stock_cd, trd_hr_mn, open_price, close_price from stock_minute_price where trd_hr_mn between '0900' and '1530' and date between ", str_replace_all(i, '-', ''), " and ", coalesce(str_replace_all(rebalancing_dates[which(rebalancing_dates==i)+1], '-', ''), '20201230'), " and stock_cd in ('", paste0(ssl_sn %>% filter(date == i) %>% slice_max(n=topN[l], order_by=get(pred_col[l])) %>% pull(stock_cd), collapse="','"),"')")) %>% mutate(date = ymd(date)) %>% group_by(stock_cd) %>% arrange(date, trd_hr_mn, .by_group=T) %>% ungroup()
        
        rets_temp1 <-
          d_stock_price %>%
          # 1. 필요한 날짜만 필터링
          filter(date >= i) %>%
          filter(date <= ifelse(i == max(rebalancing_dates),
                                ymd(end_date),
                                rebalancing_dates[which(rebalancing_dates==i)+1])) %>%
          # 2. 선택된 종목만 필터링
          filter(stock_cd %in% (ssl_sn %>% filter(date == i) %>% slice_max(n=topN[l], order_by=get(pred_col[l])) %>% pull(stock_cd))) %>%
          # 3-1. 시가
          group_by(stock_cd) %>% 
          mutate(adj_close_price = ifelse(row_number()==1, lead(adj_open_price, 1), adj_close_price)) %>% 
          mutate(price = NA)
        
        for (ticker in unique(rets_temp1$stock_cd)) {
          # 손익절 가격 설정
          upper_price = ceiling(((rets_temp1 %>% filter(stock_cd == ticker) %>% filter(date == min(date)) %>% pull(adj_close_price)) * (1+upper_bound[l])) / (1-transaction_fee_rate))
          lower_price = ceiling(((rets_temp1 %>% filter(stock_cd == ticker) %>% filter(date == min(date)) %>% pull(adj_close_price)) * (1+lower_bound[l])) / (1-transaction_fee_rate))
          
          # 필요한 틱데이터만 저장
          tic_ticker = tic_temp %>% filter(stock_cd == ticker) %>% filter(date != min(date))
          
          # 수정주가
          tic_ratio1 = tic_temp %>% filter(stock_cd == ticker) %>% dplyr::slice(1) %>% pull(open_price)
          tic_ratio2 = rets_temp1 %>% filter(stock_cd == ticker) %>% dplyr::slice(1) %>% pull(adj_open_price)
          tic_ratio = tic_ratio1 / tic_ratio2
          
          # 손익절 이벤트 첫 일어난 틱 추출
          upper_row = tic_ticker %>% mutate(adj_close_price = close_price / tic_ratio) %>% filter(adj_close_price >= upper_price) %>% dplyr::slice(1)
          lower_row = tic_ticker %>% mutate(adj_close_price = close_price / tic_ratio) %>% filter(adj_close_price <= lower_price) %>% dplyr::slice(1)
          
          if (nrow(upper_row) == 1 & nrow(lower_row) == 1) {
            
            if (ymd_hm(paste0(upper_row$date, " ", upper_row$trd_hr_mn)) < ymd_hm(paste0(lower_row$date, " ", lower_row$trd_hr_mn))) {
              sell_row = upper_row %>% mutate(price = upper_price) # A. 익절이 먼저 일어난 경우
            } else {
              sell_row = lower_row %>% mutate(price = lower_price) # B. 손절이 먼저 일어난 경우
            }
          }
          # C. 익절만 한 경우
          if (nrow(upper_row) == 1 & nrow(lower_row) == 0) sell_row = upper_row %>% mutate(price = upper_price)
          # D. 손절만 한 경우
          if (nrow(upper_row) == 0 & nrow(lower_row) == 1) sell_row = lower_row %>% mutate(price = lower_price)
          # E. 둘 다 안한 경우
          if (nrow(upper_row) == 0 & nrow(lower_row) == 0) sell_row = NA
          
          # print("==============================")
          # print(paste0("Ticker : ", ticker, " / Upper : ", upper_price, " / Lower : ", lower_price))
          # print(upper_row)
          # print(lower_row)
          # print(sell_row)
          # print("")
          
          if (sum(is.na(sell_row)) > 0) {
            rets_temp1 = rets_temp1 %>% mutate(price = ifelse(stock_cd == ticker, adj_close_price, price))
          } else {
            rets_temp1 <- 
              rets_temp1 %>% 
              left_join(sell_row %>% select(date, stock_cd, price_temp = price), by=c("date", "stock_cd")) %>% 
              # 익절/손절 후 수익률 동결
              mutate(price_temp = na.locf0(price_temp)) %>% 
              mutate(price = ifelse(stock_cd == ticker, coalesce(coalesce(price, price_temp), adj_close_price), price)) %>% 
              select(-price_temp)
          }
        }
        
        rets_temp <-
          rets_temp1 %>% 
          ungroup() %>% 
          select(stock_cd, date, price) %>% 
          # 3-4. Spread
          spread(key = "stock_cd",
                 value = "price") %>% 
          select_if(~ !any(is.na(.)))
        
        rets_temp[is.na(rets_temp)] = 0 # 상폐 처리
        ssc = ncol(rets_temp)-1
        names(rets_temp)[-1] <- paste0("stock", c(1 : ssc))
        
        # Calculate Daily Return with Tax
        rets_base <- rets_temp
        for (s in 1:ssc) {
          rets_base <- cbind(rets_base, rets_temp %$% get(paste0('stock', s))[1])
          colnames(rets_base)[ncol(rets_base)] <- paste0("base", s)
          rets_base %<>% mutate(return_temp = ( (get(paste0('stock',s)) * (1-transaction_fee_rate)) -get(paste0('base',s)) ) / get(paste0('base',s)))
          colnames(rets_base)[ncol(rets_base)] <- paste0("return", s)
        }
        
        rets_cum_temp <- rets_base %>% select(date, contains('return'))
        
        # Calculate Portfolio Return =====
        
        portfolio.returns <- c()
        
        if (sum(is.na(weight_list[l][[1]])) == 1) {
          portfolio.returns <- rets_cum_temp %>% mutate(pr = rowMeans(rets_cum_temp %>% select(-date))) %>% pull(pr) %>% unname()
        } else {
          if(sum(is.na(weight_list[[l]])) > 0) stop("If weight_list is used, it cannot contain NA.")
          portfolio.returns <- rets_cum_temp %>% mutate(pr = rowWeightedMeans(rets_cum_temp %>% select(-date) %>% as.matrix(), w=as.numeric(wm %>% filter(dates == i) %>% select(-dates)))) %>% pull(pr) %>% unname()
        }
        
        # Get Safe Haven Return =====
        if (sum(is.na(safe_haven[l][[1]])) == 1) {
          invisible()
        } else {
          safe_haven.returns <- safe_haven_price %>%
            filter(date %in% rets_cum_temp$date) %>% 
            mutate(safe_haven_return = (price-lag(price))/lag(price),
                   safe_haven_return = ifelse(is.na(safe_haven_return), 0, safe_haven_return),
                   safe_haven_cumret = cumprod(safe_haven_return+1)-1) %>% 
            select(date, safe_haven_cumret) %>% 
            pull(safe_haven_cumret)
          
          if(sum(is.na(safe_haven[[l]])) > 0) stop("If safe_haven is used, it cannot contain NA.")
          safe_haven_weight = safe_haven[[l]] %>% filter(date == i) %>% pull(w)
          
          portfolio.returns <- portfolio.returns*(1-safe_haven_weight) + safe_haven.returns*safe_haven_weight
        }
        
        # Save Cumulative Return =====
        if (nrow(rets_cum) == 0) {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date, return = portfolio.returns))
        } else {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date[-1],
                                                 return=((1+portfolio.returns[-1])*(1+rets_cum$return[nrow(rets_cum)])-1)))
        }
        
        # Extra Metrics =====
        # Monthly Win Ratio
        
        market_win_yn <-
          d_kospi_kosdaq_cum %>%
          filter(date %in% rets_cum_temp$date) %>%
          select(date, kospi, kosdaq) %>%
          #dplyr::slice(-1) %>%
          mutate(kospi_cumret = cumprod(1+kospi)-1, kosdaq_cumret = cumprod(1+kosdaq)-1) %>% 
          mutate(market_cumret = (kospi_cumret+kosdaq_cumret)/2) %$%
          market_cumret[nrow(.)] < portfolio.returns[length(portfolio.returns)]
        
        market_win_vec <- c(market_win_vec, market_win_yn)
        
        # Risk Ratio
        risk_ratio_vec <- c(risk_ratio_vec, portfolio.returns[length(portfolio.returns)])
      }
      
      # Post-work =====
      model_nm_temp =
        paste0(
          str_pad(l, side='left', width=2, pad='0'), ".",
          pred_col[l], ", ",
          "Top", topN[l], ", ",
          "SN: ", format(SN_ratio[l], nsmall = 1),
          ifelse(sum(is.na(safe_haven[l][[1]])) == 0, paste0(", Safe Haven: Y"), ""),
          ifelse(upper_bound[l]==999999 & lower_bound[l]==-999999, "", paste0(", Sell Bound: (", ifelse(lower_bound[l]==-999999, "None", lower_bound[l]), ", ", ifelse(upper_bound[l]==999999, "None", upper_bound[l]), ")")),
          ifelse(sum(is.na(weight_list[l][[1]])) == 0, paste0(", Weight: Y  ["), "  ["),
          "Hit Ratio: ", round(sum(risk_ratio_vec > 0) / length(risk_ratio_vec), 2), ", ",
          "Win Ratio: ", round(sum(market_win_vec) / length(market_win_vec), 2), ", ",
          "MDD: ", data.frame(pr = risk_ratio_vec) %>% mutate(cumret = cumprod(pr+1)) %>% mutate(preceding_max = cummax(cumret)) %>% mutate(mdd = (cumret-preceding_max)/preceding_max) %>% pull(mdd) %>% min() %>% round(2), ", ",
          "SR: ", round(mean(risk_ratio_vec) / sd(risk_ratio_vec) * sqrt(rebalancing_dates %>% substr(1, 4) %>% table() %>% median()), 2), ", ",
          "Return: ", rets_cum %>% filter(date == max(date)) %>% pull(return) %>% round(2), "]"
        )
      
      rets_total <- rbind(rets_total, rets_cum %>% mutate(model_nm = model_nm_temp))
      print(model_nm_temp)
    }
    
    # Prepare Plot =====
    rets_total <- rbind(rets_total,
                        d_kospi_kosdaq_cum %>% select(date, return=kospi_cumret) %>% mutate(model_nm = "KOSPI") %>% filter(date <= max(rets_total$date)),
                        d_kospi_kosdaq_cum %>% select(date, return=kosdaq_cumret) %>% mutate(model_nm = "KOSDAQ") %>% filter(date <= max(rets_total$date)))
    
    toc()
    
    rets_total %>% mutate(label = if_else(date == max(date), as.character(round(return,2)), NA_character_)) %>%
      ggplot(aes(x=ymd(date), y=return, col=model_nm)) +
      geom_line(size=1.1) +
      theme_minimal() +
      ggtitle("Portfolio Return") +
      theme(legend.position = c(0.01, 0.99),
            legend.justification = c(0, 1),
            legend.text=element_text(size=8),
            legend.title=element_blank(),
            legend.key = element_rect(fill="transparent", colour = "transparent"),
            legend.background = element_rect(fill="transparent", colour = "transparent")
      ) +
      scale_x_date(date_labels="%Y-%m",date_breaks  ="6 month") +
      xlab("Date") +
      ylab("Cumulative Return") +
      ylim(min(rets_total$return), max(rets_total$return)+(max(rets_total$return)*0.2)) +
      labs(caption = paste0("Backtest: ", str_replace_all(min(rets_total$date),'-','.'), ' ~ ', str_replace_all(max(rets_total$date),'-','.'), '\nCreated: ', str_replace_all(Sys.Date(),'-','.'))) +
      geom_label_repel(aes(label = label),
                       nudge_x = 1,
                       na.rm = TRUE,
                       show.legend = FALSE) +
      ggtitle(test_title)
  }

#' @export
backtest_portfolio_usa =
  function(test_title="Portfolio Return", ssl_list, topN, pred_col, upper_bound, lower_bound, start_date = '20170104', end_date = '99991231', load_data = 'Y') {

    transaction_fee_rate = 0.00315

    # Check Arugments =====
    if(length(topN) != length(ssl_list)) {
      stop("topN length must be equal to ssl_list length")
    }
    if(length(pred_col) != length(ssl_list)) {
      stop("pred_col length must be equal to ssl_list length")
    }
    if(length(upper_bound) != length(ssl_list)) {
      stop("upper_bound length must be equal to ssl_list length")
    }
    if(length(lower_bound) != length(ssl_list)) {
      stop("lower_bound length must be equal to ssl_list length")
    }
    upper_bound[is.na(upper_bound)] = 999999
    lower_bound[is.na(lower_bound)] = -999999
    if(sum(upper_bound <= 0) > 0) {
      stop("Upper selling bound must be greater than Zero")
    }
    if(sum(lower_bound >= 0) > 0) {
      stop("Lower selling lower bound must be less than Zero")
    }

    # Load Data if Needed =====
    if(load_data == 'Y') {
      library(RMySQL)
      conn <- dbConnect(
        MySQL(),
        user = 'betterlife',
        password = 'snail132',
        host = 'betterlife.duckdns.org',
        port = 1231,
        dbname = 'stock_usa'
      )
      dbSendQuery(conn, "SET NAMES utf8;")
      dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
      dbSendQuery(conn, "SET character_set_connection=utf8mb4;")

      # Stock Price
      d_stock_price <- dbGetQuery(conn, paste0("select * from stock_usa.stock_adj_price where date >= '", start_date, "';"))
      # snp500 & nasdaq
      d_snp500_nasdaq <- dbGetQuery(conn, "select date, `s&p_500_composite_pt` as snp500, `nasdaq_composite_pt` as nasdaq from macro_global where date >= '20150101';")

      # Disconnect MySQL Server
      lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
    }

    tic()

    # Prepare Data =====
    d_stock_price %<>%
      mutate(date=ymd(date)) %>%
      mutate(adj_low_price = ifelse(is.na(adj_low_price), adj_close_price, adj_low_price)) %>%
      mutate(adj_high_price = ifelse(is.na(adj_high_price), adj_close_price, adj_high_price)) %>%
      filter(date <= ymd(end_date))
    d_snp500_nasdaq_cum <-
      d_snp500_nasdaq %>%
      mutate(date = ymd(date)) %>%
      arrange(date) %>%
      mutate(snp500 = (snp500-lag(snp500))/lag(snp500),
             nasdaq = (nasdaq - lag(nasdaq))/lag(nasdaq)) %>%
      na.omit() %>%
      filter(date >= ymd(start_date)) %>%
      filter(date <= ymd(end_date)) %>%
      mutate(snp500_cumret = cumprod(snp500+1)-1, nasdaq_cumret = cumprod(nasdaq+1)-1)

    # Start Simulation =====
    rets_total <- data.frame()
    for (l in 1:length(ssl_list)) {
      # Pre-work =====
      ssl <-
        ssl_list[[l]] %>%
        mutate(date = ymd(date)) %>%
        filter(date >= ymd(start_date) & date <= ymd(end_date)) %>%
        group_by(date) %>%
        select(date, stock_cd, pred_col[l]) %>%
        arrange(desc(get(pred_col[l])), .by_group = TRUE) %>%
        ungroup()

      # Sector Neutral =====
      ssl_sn <- ssl
      rebalancing_dates <- unique(ssl$date)

      rets_cum <- data.frame()
      market_win_vec <- c()
      risk_ratio_vec <- c()

      # Work =====
      for(k in rebalancing_dates) {
        i = as.Date(k, origin = '1970-01-01')

        # Calculate Each Stock Return =====

        # Get Stock Price of Selected Stocks
        rets_temp <-
          d_stock_price %>%
          # 1. 필요한 날짜만 필터링
          filter(date >= i) %>%
          filter(date <= ifelse(i == max(rebalancing_dates),
                                ymd(end_date),
                                rebalancing_dates[which(rebalancing_dates==i)+1])) %>%
          # 2. 선택된 종목만 필터링
          filter(stock_cd %in% (ssl_sn %>% filter(date == i) %>% slice_max(n=topN[l], order_by=get(pred_col[l])) %>% pull(stock_cd))) %>%
          # 3-1. 익절/손절 가격 설정
          group_by(stock_cd) %>%
          mutate(upper_price = ceiling((adj_close_price[1]*(1+upper_bound[l]))/(1-transaction_fee_rate)),
                 lower_price = ceiling((adj_close_price[1]*(1+lower_bound[l]))/(1-transaction_fee_rate))) %>%
          # 3-2. 익절/손절 여부 태깅
          mutate(sell_cd = ifelse(lag(adj_high_price) > upper_price | lag(adj_low_price) < lower_price, NA, 1)) %>%
          mutate(sell_cd = cumprod(ifelse(row_number()==1, 1, sell_cd))) %>%
          # 3-3. 익절/손절 후 수익률 동결
          mutate(price = case_when(adj_high_price > upper_price ~ upper_price*sell_cd,
                                   adj_low_price < lower_price ~ lower_price*sell_cd,
                                   TRUE ~ adj_close_price*sell_cd)) %>%
          mutate(price = na.locf(price)) %>%
          ungroup() %>%
          select(stock_cd, date, price) %>%
          # 3-4. Spread
          spread(key = "stock_cd",
                 value = "price")
        rets_temp[is.na(rets_temp)] = 0 # 상폐 처리
        ssc = ncol(rets_temp)-1
        names(rets_temp)[-1] <- paste0("stock", c(1 : ssc))

        # Calculate Daily Return with Tax
        rets_base <- rets_temp
        for (s in 1:ssc) {
          rets_base <- cbind(rets_base, rets_temp %$% get(paste0('stock', s))[1])
          colnames(rets_base)[ncol(rets_base)] <- paste0("base", s)
          rets_base %<>% mutate(return_temp = ( (get(paste0('stock',s)) * (1-transaction_fee_rate)) -get(paste0('base',s)) ) / get(paste0('base',s)))
          colnames(rets_base)[ncol(rets_base)] <- paste0("return", s)
        }

        rets_cum_temp <- rets_base %>% select(date, contains('return'))

        # Calculate Portfolio Return =====

        portfolio.returns <- rets_cum_temp %>% mutate(pr = rowMeans(rets_cum_temp %>% select(-date))) %>% pull(pr) %>% unname()

        # Save Cumulative Return =====
        if (nrow(rets_cum) == 0) {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date, return = portfolio.returns))
        } else {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date[-1],
                                                 return=((1+portfolio.returns[-1])*(1+rets_cum$return[nrow(rets_cum)])-1)))
        }

        # Extra Metrics =====
        # Monthly Win Ratio

        market_win_yn <-
          d_snp500_nasdaq_cum %>%
          filter(date %in% rets_cum_temp$date) %>%
          select(date, snp500, nasdaq) %>%
          #dplyr::slice(-1) %>%
          mutate(snp500_cumret = cumprod(1+snp500)-1, nasdaq_cumret = cumprod(1+nasdaq)-1) %>%
          mutate(market_cumret = (snp500_cumret+nasdaq_cumret)/2) %$%
          market_cumret[nrow(.)] < portfolio.returns[length(portfolio.returns)]

        market_win_vec <- c(market_win_vec, market_win_yn)

        # Risk Ratio
        risk_ratio_vec <- c(risk_ratio_vec, portfolio.returns[length(portfolio.returns)])
      }

      # Post-work =====
      model_nm_temp =
        paste0(
          str_pad(l, side='left', width=2, pad='0'), ".",
          pred_col[l], ", ",
          "Top", topN[l],
          ifelse(upper_bound[l]==999999 & lower_bound[l]==-999999, " [", paste0(", Sell Bound: (", ifelse(lower_bound[l]==-999999, "None", lower_bound[l]), ", ", ifelse(upper_bound[l]==999999, "None", upper_bound[l]), ") [")),
          "Win Ratio: ", round(sum(market_win_vec) / length(market_win_vec), 2), ", ",
          "Hit Ratio: ", round(sum(risk_ratio_vec > 0) / length(risk_ratio_vec), 2), ", ",
          "Stability: ", round(mean(risk_ratio_vec) / sd(risk_ratio_vec), 2), ", ",
          "Return: ", rets_cum %>% filter(date == max(date)) %>% pull(return) %>% round(2), "]"
        )

      rets_total <- rbind(rets_total, rets_cum %>% mutate(model_nm = model_nm_temp))
      print(model_nm_temp)
    }

    # Prepare Plot =====
    rets_total <- rbind(rets_total,
                        d_snp500_nasdaq_cum %>% select(date, return=snp500_cumret) %>% mutate(model_nm = "snp500") %>% filter(date <= max(rets_total$date)),
                        d_snp500_nasdaq_cum %>% select(date, return=nasdaq_cumret) %>% mutate(model_nm = "nasdaq") %>% filter(date <= max(rets_total$date)))

    toc()

    rets_total %>% mutate(label = if_else(date == max(date), as.character(round(return,2)), NA_character_)) %>%
      ggplot(aes(x=ymd(date), y=return, col=model_nm)) +
      geom_line(size=1.1) +
      theme_minimal() +
      ggtitle("Portfolio Return") +
      theme(legend.position = c(0.01, 0.99),
            legend.justification = c(0, 1),
            legend.text=element_text(size=8),
            legend.title=element_blank(),
            legend.key = element_rect(fill="transparent", colour = "transparent"),
            legend.background = element_rect(fill="transparent", colour = "transparent")
      ) +
      scale_x_date(date_labels="%Y-%m",date_breaks  ="6 month") +
      xlab("Date") +
      ylab("Cumulative Return") +
      ylim(min(rets_total$return), max(rets_total$return)+(max(rets_total$return)*0.2)) +
      labs(caption = paste0(min(rets_total$date), ' / ', max(rets_total$date))) +
      geom_label_repel(aes(label = label),
                       nudge_x = 1,
                       na.rm = TRUE,
                       show.legend = FALSE) +
      ggtitle(test_title)
  }

#' @export
backtest_portfolio_vn =
  function(test_title="Portfolio Return", ssl_list, topN, pred_col, upper_bound, lower_bound, start_date = '20170104', end_date = '99991231', load_data = 'Y') {

    transaction_fee_rate = 0.00315

    # Check Arugments =====
    if(length(topN) != length(ssl_list)) {
      stop("topN length must be equal to ssl_list length")
    }
    if(length(pred_col) != length(ssl_list)) {
      stop("pred_col length must be equal to ssl_list length")
    }
    if(length(upper_bound) != length(ssl_list)) {
      stop("upper_bound length must be equal to ssl_list length")
    }
    if(length(lower_bound) != length(ssl_list)) {
      stop("lower_bound length must be equal to ssl_list length")
    }
    upper_bound[is.na(upper_bound)] = 999999
    lower_bound[is.na(lower_bound)] = -999999
    if(sum(upper_bound <= 0) > 0) {
      stop("Upper selling bound must be greater than Zero")
    }
    if(sum(lower_bound >= 0) > 0) {
      stop("Lower selling lower bound must be less than Zero")
    }

    # Load Data if Needed =====
    if(load_data == 'Y') {
      library(RMySQL)
      conn <- dbConnect(MySQL(), user = "betterlife", password = "snail132",
                        host = "betterlife.duckdns.org", port = 1231, dbname = "stock_vn")
      dbSendQuery(conn, "SET NAMES utf8;")
      dbSendQuery(conn, "SET CHARACTER SET utf8mb4;")
      dbSendQuery(conn, "SET character_set_connection=utf8mb4;")

      # Stock Price
      d_stock_price <- dbGetQuery(conn, paste0("select * from stock_vn.stock_adj_price where date >= '", start_date, "';"))
      # vn & hnx
      d_vn_hnx <- dbGetQuery(conn, "select date, `vn`, `hnx` from stock_vn.macro_emerging where date >= '20150101';")

      # Disconnect MySQL Server
      lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
    }

    tic()

    # Prepare Data =====
    d_stock_price %<>%
      mutate(date=ymd(date)) %>%
      mutate(adj_low_price = ifelse(is.na(adj_low_price), adj_close_price, adj_low_price)) %>%
      mutate(adj_high_price = ifelse(is.na(adj_high_price), adj_close_price, adj_high_price)) %>%
      filter(date <= ymd(end_date))
    d_vn_hnx_cum <-
      d_vn_hnx %>%
      mutate(date = ymd(date)) %>%
      arrange(date) %>%
      mutate(vn = (vn-lag(vn))/lag(vn),
             hnx = (hnx - lag(hnx))/lag(hnx)) %>%
      na.omit() %>%
      filter(date >= ymd(start_date)) %>%
      filter(date <= ymd(end_date)) %>%
      mutate(vn_cumret = cumprod(vn+1)-1, hnx_cumret = cumprod(hnx+1)-1)

    # Start Simulation =====
    rets_total <- data.frame()
    for (l in 1:length(ssl_list)) {
      # Pre-work =====
      ssl <-
        ssl_list[[l]] %>%
        mutate(date = ymd(date)) %>%
        filter(date >= ymd(start_date) & date <= ymd(end_date)) %>%
        group_by(date) %>%
        select(date, stock_cd, pred_col[l]) %>%
        arrange(desc(get(pred_col[l])), .by_group = TRUE) %>%
        ungroup()

      # Sector Neutral =====
      ssl_sn <- ssl
      rebalancing_dates <- unique(ssl$date)

      rets_cum <- data.frame()
      market_win_vec <- c()
      risk_ratio_vec <- c()

      # Work =====
      for(k in rebalancing_dates) {
        i = as.Date(k, origin = '1970-01-01')

        # Calculate Each Stock Return =====

        # Get Stock Price of Selected Stocks
        rets_temp <-
          d_stock_price %>%
          # 1. 필요한 날짜만 필터링
          filter(date >= i) %>%
          filter(date <= ifelse(i == max(rebalancing_dates),
                                ymd(end_date),
                                rebalancing_dates[which(rebalancing_dates==i)+1])) %>%
          # 2. 선택된 종목만 필터링
          filter(stock_cd %in% (ssl_sn %>% filter(date == i) %>% slice_max(n=topN[l], order_by=get(pred_col[l])) %>% pull(stock_cd))) %>%
          # 3-1. 익절/손절 가격 설정
          group_by(stock_cd) %>%
          mutate(upper_price = ceiling((adj_close_price[1]*(1+upper_bound[l]))/(1-transaction_fee_rate)),
                 lower_price = ceiling((adj_close_price[1]*(1+lower_bound[l]))/(1-transaction_fee_rate))) %>%
          # 3-2. 익절/손절 여부 태깅
          mutate(sell_cd = ifelse(lag(adj_high_price) > upper_price | lag(adj_low_price) < lower_price, NA, 1)) %>%
          mutate(sell_cd = cumprod(ifelse(row_number()==1, 1, sell_cd))) %>%
          # 3-3. 익절/손절 후 수익률 동결
          mutate(price = case_when(adj_high_price > upper_price ~ upper_price*sell_cd,
                                   adj_low_price < lower_price ~ lower_price*sell_cd,
                                   TRUE ~ adj_close_price*sell_cd)) %>%
          mutate(price = na.locf(price)) %>%
          ungroup() %>%
          select(stock_cd, date, price) %>%
          # 3-4. Spread
          spread(key = "stock_cd",
                 value = "price")
        rets_temp[is.na(rets_temp)] = 0 # 상폐 처리
        ssc = ncol(rets_temp)-1
        names(rets_temp)[-1] <- paste0("stock", c(1 : ssc))

        # Calculate Daily Return with Tax
        rets_base <- rets_temp
        for (s in 1:ssc) {
          rets_base <- cbind(rets_base, rets_temp %$% get(paste0('stock', s))[1])
          colnames(rets_base)[ncol(rets_base)] <- paste0("base", s)
          rets_base %<>% mutate(return_temp = ( (get(paste0('stock',s)) * (1-transaction_fee_rate)) -get(paste0('base',s)) ) / get(paste0('base',s)))
          colnames(rets_base)[ncol(rets_base)] <- paste0("return", s)
        }

        rets_cum_temp <- rets_base %>% select(date, contains('return'))

        # Calculate Portfolio Return =====

        portfolio.returns <- rets_cum_temp %>% mutate(pr = rowMeans(rets_cum_temp %>% select(-date))) %>% pull(pr) %>% unname()

        # Save Cumulative Return =====
        if (nrow(rets_cum) == 0) {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date, return = portfolio.returns))
        } else {
          rets_cum <- rbind(rets_cum, data.frame(date=rets_cum_temp$date[-1],
                                                 return=((1+portfolio.returns[-1])*(1+rets_cum$return[nrow(rets_cum)])-1)))
        }

        # Extra Metrics =====
        # Monthly Win Ratio

        market_win_yn <-
          d_vn_hnx_cum %>%
          filter(date %in% rets_cum_temp$date) %>%
          select(date, vn, hnx) %>%
          #dplyr::slice(-1) %>%
          mutate(vn_cumret = cumprod(1+vn)-1, hnx_cumret = cumprod(1+hnx)-1) %>%
          mutate(market_cumret = (vn_cumret+hnx_cumret)/2) %$%
          market_cumret[nrow(.)] < portfolio.returns[length(portfolio.returns)]

        market_win_vec <- c(market_win_vec, market_win_yn)

        # Risk Ratio
        risk_ratio_vec <- c(risk_ratio_vec, portfolio.returns[length(portfolio.returns)])
      }

      # Post-work =====
      model_nm_temp =
        paste0(
          str_pad(l, side='left', width=2, pad='0'), ".",
          pred_col[l], ", ",
          "Top", topN[l],
          ifelse(upper_bound[l]==999999 & lower_bound[l]==-999999, " [", paste0(", Sell Bound: (", ifelse(lower_bound[l]==-999999, "None", lower_bound[l]), ", ", ifelse(upper_bound[l]==999999, "None", upper_bound[l]), ") [")),
          "Win Ratio: ", round(sum(market_win_vec) / length(market_win_vec), 2), ", ",
          "Hit Ratio: ", round(sum(risk_ratio_vec > 0) / length(risk_ratio_vec), 2), ", ",
          "Stability: ", round(mean(risk_ratio_vec) / sd(risk_ratio_vec), 2), ", ",
          "Return: ", rets_cum %>% filter(date == max(date)) %>% pull(return) %>% round(2), "]"
        )

      rets_total <- rbind(rets_total, rets_cum %>% mutate(model_nm = model_nm_temp))
      print(model_nm_temp)
    }

    # Prepare Plot =====
    rets_total <- rbind(rets_total,
                        d_vn_hnx_cum %>% select(date, return=vn_cumret) %>% mutate(model_nm = "vn") %>% filter(date <= max(rets_total$date)),
                        d_vn_hnx_cum %>% select(date, return=hnx_cumret) %>% mutate(model_nm = "hnx") %>% filter(date <= max(rets_total$date)))

    toc()

    rets_total %>% mutate(label = if_else(date == max(date), as.character(round(return,2)), NA_character_)) %>%
      ggplot(aes(x=ymd(date), y=return, col=model_nm)) +
      geom_line(size=1.1) +
      theme_minimal() +
      ggtitle("Portfolio Return") +
      theme(legend.position = c(0.01, 0.99),
            legend.justification = c(0, 1),
            legend.text=element_text(size=8),
            legend.title=element_blank(),
            legend.key = element_rect(fill="transparent", colour = "transparent"),
            legend.background = element_rect(fill="transparent", colour = "transparent")
      ) +
      scale_x_date(date_labels="%Y-%m",date_breaks  ="6 month") +
      xlab("Date") +
      ylab("Cumulative Return") +
      ylim(min(rets_total$return), max(rets_total$return)+(max(rets_total$return)*0.2)) +
      labs(caption = paste0(min(rets_total$date), ' / ', max(rets_total$date))) +
      geom_label_repel(aes(label = label),
                       nudge_x = 1,
                       na.rm = TRUE,
                       show.legend = FALSE) +
      ggtitle(test_title)
  }

#' @export
return_tile = function(ssl_input, topN, pred_col) {

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
    ssl_input %>%
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
    filter(date <= Sys.Date()-30) %>%
    dplyr::slice(1:topN) %>%
    ungroup() %>%
    mutate(target_1m_return = ifelse(is.na(target_1m_return), -1, target_1m_return)) %>%
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
