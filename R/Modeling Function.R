# LGBM Ensemble Modeling Function (Base)
# Return and Save Selected Stock List (SSL)

#' @export
modeling_func = function(df, target_y, title = "", num_threads_params=12, train_span=36, push_span=1, ensemble_n = 300, bagging_prop=0.8, feature_prop=0.8, num_rounds=60, pred_start_date = '2015-01-01', explain_yn = 'N', focal_loss_yn = 'N') {
  
  pred_start_date = ymd(pred_start_date)
  
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
  return(ssl)
}

#' @export
modeling_func_parallel = function(df, target_y, title = "", train_span=36, push_span=1, ensemble_n = 300, bagging_prop=0.8, feature_prop=0.8, num_rounds=60, pred_start_date = '2017-01-01', explain_yn = 'Y') {

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
      objective = "binary",
      bagging_fraction = bagging_prop,
      feature_fraction = feature_prop,
      bagging_freq = 1
    )

  for (i in seq(1, by=push_span, to = length(date_unique_bt) - train_span)) {

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

    lgbm_test_dat <- data.matrix(dt_test %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return'))))

    # Start Training
    pred_mat <- matrix(nrow=nrow(dt_test), ncol=ensemble_n)
    cl = parallel::makeCluster(4)
    doParallel::registerDoParallel(cl)
    clusterEvalQ(cl, {
      library(lightgbm)
      # no additional packages here
    })

    pred_mat = foreach(m = 1:ensemble_n,
                       .packages = c("lightgbm","Matrix","dplyr"),
                       .combine = cbind) %dopar% {

                         # Make Matrix
                         lgbm_train_dat <-
                           lgb.Dataset(
                             data.matrix(dt_train %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))),
                             label = dt_train[,target_y]
                           )

                         lgbm_model <-
                           lightgbm(
                             params = lgbm_params,
                             objective = "binary",
                             data = lgbm_train_dat,
                             boosting = "gbdt",
                             nrounds = num_rounds,
                             tree_learner = "voting",
                             verbose = -1,
                             seed = m
                           )

                         # Predict
                         pred_mat <- predict(lgbm_model, lgbm_test_dat)

                       }

    stopCluster(cl)

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

      lgbm_train_dat <-
        lgb.Dataset(
          data.matrix(dt_train %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))),
          label = dt_train[,target_y]
        )

      lgbm_model <-
        lightgbm(
          params = lgbm_params,
          objective = "binary",
          data = lgbm_train_dat,
          boosting = "gbdt",
          nrounds = num_rounds,
          tree_learner = "voting",
          verbose = -1
        )

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
  }
  ssl <<- data.frame()
  for (i in 1:length(selected_stock_cd_list)) {
    ssl <<- rbind(ssl, selected_stock_cd_list[[i]])
  }
  ssl %<>% left_join(df %>% select(date, stock_cd, target_1m_return), by = c('date', 'stock_cd'))
  saveRDS(ssl, paste0("ssl_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y," (n", ensemble_n, ").RDS"))
  saveRDS(shap_train_df, paste0("trainSHAP_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y, ".RDS"))
  saveRDS(shap_test_df %>% left_join(ssl %>% select(date, stock_cd, pred_mean), by=c("date", "stock_cd")), paste0("testSHAP_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y, ".RDS"))
  return(ssl)
}

#' @export
market_updown_pred <- function(ssl_wo_macro, lookup_span) {
  
  # Get Market =====
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
  korea_return = 
    dbGetQuery(conn, "select * from stock_kospi_kosdaq where date >= '20101201'") %>% 
    mutate(date = ymd(date)) %>% 
    filter(date %in% unique(ssl_wo_macro$date)) %>% 
    select(date, kospi, kosdaq) %>% 
    mutate(kospi = (lead(kospi)-kospi)/kospi,
           kosdaq = (lead(kosdaq)-kosdaq)/kosdaq) %>% 
    mutate(korea = (kospi+kosdaq)/2) %>% 
    select(date, korea) # calculate korean market leading return
  lapply(dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
  
  rebalancing_dates = unique(korea_return$date)
  
  market_pred_df = data.frame()
  # Start Process =====
  for (i in (1:(length(rebalancing_dates)-lookup_span))) {
    ssl_temp = ssl_wo_macro %>% filter(date >= rebalancing_dates[i] & date <= rebalancing_dates[i+lookup_span-1])
    
    # 1. Find Threshold =====
    threshold_df = 
      ssl_temp %>% 
      group_by(date) %>% 
      # arrange(desc(pred_mean), .by_group=T) %>% 
      # dplyr::slice(1:100) %>%
      summarize(prob_mean = mean(pred_mean)) %>%
      inner_join(korea_return, by="date") %>% 
      mutate(korea_updown = ifelse(korea > 0, 1, 0)) %>% 
      select(date, prob_mean, korea_updown)
    
    th_temp = roc(threshold_df$korea_updown, threshold_df$prob_mean, quiet=T)
    opt_th = coords(th_temp, "best", ret = "threshold", transpose = T) %>% as.numeric()
    
    # 2. Prediction =====
    nxt_month_prob_mean = 
      ssl_wo_macro %>% 
      filter(date == rebalancing_dates[i+lookup_span]) %>% 
      # arrange(desc(pred_mean), .by_group=T) %>% 
      # dplyr::slice(1:100) %>% 
      summarize(prob_mean = mean(pred_mean)) %>% 
      pull(prob_mean)
    
    market_pred_df <-
      rbind(market_pred_df,
            data.frame(date = rebalancing_dates[i+lookup_span], 
                       prob_mean = nxt_month_prob_mean,
                       threshold = opt_th,
                       pred = as.numeric(nxt_month_prob_mean >= opt_th),
                       korea_updown = ifelse((korea_return %>% filter(date == rebalancing_dates[i+lookup_span]) %>% pull(korea)) > 0, 1, 0)
            ))
    print(rebalancing_dates[i+lookup_span])
  }
  return(market_pred_df)
}
