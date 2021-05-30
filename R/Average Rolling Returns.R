# Average Rolling Return

#' @export
rollret = function(ssl, top_n = 10, roll_period = 6, ensemble_n = 50, boot_n = 50, seed = 0){

  set.seed(seed)

  result = tibble()
  for( iter in 1:boot_n){

    ref = ssl %>% select(date,stock_cd,target_1m_return)
    cols = sample(3:302,ensemble_n, replace = T)
    ref[,paste0('pred_mean_',iter)] = rowMeans(ssl[,cols,with=F])

    temp = ref %>% group_by(date) %>% arrange(-get(paste0('pred_mean_',iter))) %>%
      dplyr::slice(1:top_n) %>% summarise(ret = mean(target_1m_return) + 1) %>%
      mutate(ret_roll = rollapply(ret,roll_period,prod, align = 'right', fill = NA)) %>%
      summarise(roll_avg = mean(ret_roll, na.rm = T),
                roll_min = min(ret_roll , na.rm = T),
                roll_sd = sd(ret_roll, na.rm = T)) %>%
      mutate(sample = paste0('sample_',iter))

    result = bind_rows(result,temp)
  }

  final = result %>% summarise(return = mean(roll_avg), return_min = mean(roll_min), return_sd = mean(roll_sd))
  return(final)

}


# Average Rolling Return + Mixing Targets

#' @export
rollret_mix = function(index, target, top_n = 10, roll_period = 6, ensemble_n = 50, boot_n = 50, seed = 0, nthread = 2){

  cl <- makeCluster(nthread)
  registerDoParallel(cl)
  set.seed(seed)

  result =
    foreach( iter = 1:boot_n,
             .packages = c("dplyr","zoo","magrittr",'reshape2'),
             .combine = rbind) %dopar% {

               index_cols = sample(3:302,ensemble_n, replace = T)
               target_cols = sample(3:302,ensemble_n, replace = T)

               mix = index %>% select(date,stock_cd,target_1m_return)
               target = index %>% select(date,stock_cd) %>% left_join(target, by = c('date','stock_cd'))
               mix = cbind(mix, cbind(rowMeans(index[,index_cols,with=F]),rowMeans(target[,target_cols,with=F])) %*%
                             t(cbind(seq(0,1,0.1),1 - seq(0,1,0.1))) %>% as_tibble())
               mix = mix %>% reshape2::melt(id.vars = c('date', 'stock_cd', 'target_1m_return')) %>%
                 mutate(wt = (as.numeric(gsub('V','',variable))-1)/10) %>% select(-variable) %>% rename(pred = value)

               mix =
                 mix %>% group_by(date,wt) %>% arrange(desc(pred)) %>% dplyr::slice(1:top_n) %>%
                 summarise(ret = mean(target_1m_return)+1,.groups = 'keep') %>%
                 group_by(wt) %>% arrange(date) %>%
                 mutate(ret_roll = rollapply(ret,roll_period,prod, align = 'right', fill = NA)) %>%
                 summarise(return = mean(ret_roll, na.rm = T),
                           return_min = min(ret_roll , na.rm = T),
                           return_sd = sd(ret_roll, na.rm = T))

               best_wt = mix %>% filter(return == max(return)) %>% pull(wt)
               mix =
                 mix %>% filter(wt %in% c(0,1)) %>% mutate(option = ifelse(wt == 0, 'target', 'index')) %>%
                 bind_rows(mix %>% filter(wt %in% c(round(best_wt - 0.1,1), round(best_wt,1), round(best_wt + 0.1,1))) %>%
                             summarise_at(.vars = vars(return, return_min, return_sd),.funs = mean,na.rm = T) %>%
                             mutate(wt = best_wt, option = 'best'))

             }

  baselines = result %>% filter(option %in% c('target','index')) %>% group_by(option) %>% summarise_all(mean)
  mixed = result %>% filter(option %in% c('best')) %>% group_by(option) %>% summarise_all(mean)
  mixed %<>% mutate(weight_sd = result %>% filter(option %in% c('best')) %>% pull(wt) %>% sd)
  final = bind_rows(baselines, mixed) %>% rename(weight = wt)

  stopCluster(cl)

  return(final)

}
