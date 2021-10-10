# Average Rolling Return

#' @export
rollret = function(ssl, top_n = 10, roll_period = 6, ensemble_n = 50, boot_n = 50, seed = 0){

  set.seed(seed)

  result = tibble()
  for( iter in 1:boot_n){

    ref = ssl %>% select(date,stock_cd,target_1m_return)
    cols = sample(colnames(ssl)[str_detect(colnames(ssl), "pred") & !str_detect(colnames(ssl), "mean")], ensemble_n, replace = T)
    ref[,paste0('pred_mean_',iter)] = rowMeans(ssl %>% select(all_of(cols)))

    temp = ref %>% mutate(target_1m_return = ifelse(target_1m_return>0.5,0.5,target_1m_return)) %>%
      group_by(date) %>% arrange(desc(get(paste0('pred_mean_',iter)))) %>%
      dplyr::slice(1:top_n) %>%
      summarise(ret = target_1m_return + 1) %>%
      mutate(ret_roll = rollapply(ret,roll_period,prod, align = 'right', fill = NA)) %>%
      summarise(roll_avg = mean(ret_roll-1, na.rm = T),
                roll_min = min(ret_roll-1 , na.rm = T),
                roll_sd = sd(ret_roll-1, na.rm = T)) %>%
      mutate(sample = paste0('sample_',iter))

    result = bind_rows(result,temp)
  }

  final = result %>% summarise(return = mean(roll_avg), return_min = mean(roll_min), return_sd = mean(roll_sd))
  return(final)

}


# Average Rolling Return + Mixing Targets

#' @export
rollret_mix = function(ssl1, ssl2, top_n = 10, roll_period = 6, ensemble_n = 50, boot_n = 50, seed = 0, nthread = 2){

  registerDoParallel(cores=nthread)
  set.seed(seed)

  result =
    foreach( iter = 1:boot_n,
             .packages = c("dplyr","zoo","magrittr",'reshape2','stringr'),
             .combine = rbind) %dopar% {

               ssl1_cols = sample(colnames(ssl1)[str_detect(colnames(ssl1), "pred") & !str_detect(colnames(ssl1), "mean")], ensemble_n, replace = T)
               ssl2_cols = sample(colnames(ssl2)[str_detect(colnames(ssl2), "pred") & !str_detect(colnames(ssl2), "mean")], ensemble_n, replace = T)

               mix = ssl1 %>% select(date,stock_cd,target_1m_return)
               ssl2 = ssl1 %>% select(date,stock_cd) %>% left_join(ssl2, by = c('date','stock_cd'))
               mix = cbind(mix, cbind(rowMeans(ssl1 %>% select(all_of(ssl1_cols))),rowMeans(ssl2 %>% select(all_of(ssl2_cols)))) %*%
                             t(cbind(seq(0,1,0.1),1 - seq(0,1,0.1))) %>% as_tibble())
               mix = mix %>% reshape2::melt(id.vars = c('date', 'stock_cd', 'target_1m_return')) %>%
                 mutate(wt = (as.numeric(gsub('V','',variable))-1)/10) %>% select(-variable) %>% rename(pred = value)

               mix =
                 mix %>% mutate(target_1m_return = ifelse(target_1m_return>0.5,0.5,target_1m_return)) %>%
                 group_by(date,wt) %>% arrange(desc(pred)) %>% dplyr::slice(1:top_n) %>%
                 summarise(ret = mean(target_1m_return)+1) %>%
                 group_by(wt) %>% arrange(date, .by_group=T) %>%
                 mutate(ret_roll = rollapply(ret,roll_period,prod, align = 'right', fill = NA)) %>%
                 summarise(return = mean(ret_roll-1, na.rm = T),
                           return_min = min(ret_roll-1 , na.rm = T),
                           return_sd = sd(ret_roll-1, na.rm = T))

               best_wt = mix %>% filter(return == max(return)) %>% pull(wt)
               mix =
                 mix %>% filter(wt %in% c(0,1)) %>% mutate(option = ifelse(wt == 0, 'ssl2', 'ssl1')) %>%
                 bind_rows(mix %>% filter(wt %in% c(round(best_wt - 0.1,1), round(best_wt,1), round(best_wt + 0.1,1))) %>%
                             summarise_at(.vars = vars(return, return_min, return_sd),.funs = mean,na.rm = T) %>%
                             mutate(wt = best_wt, option = 'best'))

             }

  baselines = result %>% filter(option %in% c('ssl2','ssl1')) %>% group_by(option) %>% summarise_all(mean)
  mixed = result %>% filter(option %in% c('best')) %>% group_by(option) %>% summarise_all(mean)
  mixed %<>% mutate(weight_sd = result %>% filter(option %in% c('best')) %>% pull(wt) %>% sd)
  final = bind_rows(baselines, mixed) %>% rename(weight = wt)

  return(final)

}
