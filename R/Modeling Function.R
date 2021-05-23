
# LGBM Ensemble Modeling Function (Base)
# Return and Save Selected Stock List (SSL)

#' @export
modeling_func = function(df, target_y, title = "", train_span=36, push_span=1, ensemble_n = 300, bagging_prop=0.8, feature_prop=0.8, num_rounds=60, pred_start_date = '2017-01-01') {

  pred_list <- list()
  rmse_valid_vec = c()
  selected_stock_cd_list = list()
  k = 1
  j=1

  # Set Data Span
  data_train <-
    df %>%
    filter(date < pred_start_date)
  data_test <-
    df %>%
    filter(date >= pred_start_date)
  date_unique <- unique(data_train$date)
  data_bt <-
    rbind(data_train %>% filter(between(date, date_unique[length(date_unique)-train_span+1], date_unique[length(date_unique)])),
          data_test)
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

    # Make Matrix
    lgbm_train_dat <-
      lgb.Dataset(
        Matrix(
          as.matrix(dt_train %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))), sparse = TRUE),
        label = dt_train[,target_y]
      )

    train_y <- dt_train[,target_y]

    lgbm_test_dat <-
      Matrix(
        as.matrix(dt_test %>% select(-c(`stock_cd`,`date`, contains('target'), contains('return')))), sparse = TRUE)

    # Start Training
    pred_mat <- matrix(nrow=nrow(dt_test), ncol=ensemble_n)

    for (m in 1:ensemble_n) {
      lgbm_model <-
        lightgbm(
          params = lgbm_params,
          data = lgbm_train_dat,
          boosting = "gbdt",
          nrounds = num_rounds,
          tree_learner = "voting",
          verbose = -1,
          seed = m
        )

      # Predict
      lgbm_pred <- predict(lgbm_model, lgbm_test_dat)
      pred_mat[,m] <- lgbm_pred
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

    # saving on 'selected_stock_cd_list'
    selected_stock_cd_list[[k]] <- top_list

    k = k+1

    print(top_list$date %>% unique())
  }
  ssl <<- data.frame()
  for (i in 1:length(selected_stock_cd_list)) {
    ssl <<- rbind(ssl, selected_stock_cd_list[[i]])
  }
  saveRDS(ssl, paste0("ssl_",str_replace_all(Sys.Date(), '-', ''), "_",title, "_", target_y,"_n", ensemble_n, ".RDS"))
  return(ssl)
}
