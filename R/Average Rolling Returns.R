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

  return(result)

}


# Average Rolling Return + Mixing Targets

#' @export
rollret_mix = function(index, target, top_n = 10, roll_period = 6, ensemble_n = 50, boot_n = 50, seed = 0){

  set.seed(seed)

  result = tibble()
  for( iter in 1:boot_n){

    index_ref = index %>% select(date,stock_cd,target_1m_return)
    index_cols = sample(3:302,ensemble_n, replace = T)
    index_ref[,paste0('pred_mean_',iter)] = rowMeans(index[,index_cols,with=F])

    target_ref = index %>% select(date,stock_cd,target_1m_return) %>% left_join(target, by = c('date','stock_cd','target_1m_return'))
    target_cols = sample(3:302,ensemble_n, replace = T)
    target_ref[,paste0('pred_mean_',iter)] = rowMeans(target[,target_cols,with=F])

    mix = index %>% select(date,stock_cd,target_1m_return)
    target = index %>% select(date,stock_cd,target_1m_return) %>% left_join(target, by = c('date','stock_cd','target_1m_return'))
    mix = cbind(mix, cbind(rowMeans(index[,index_cols,with=F]),rowMeans(target[,target_cols,with=F])) %*%
                  t(cbind(seq(0,1,0.1),1 - seq(0,1,0.1))) %>% as_tibble())
    mix = mix %>% melt(id.vars = c('date', 'stock_cd', 'target_1m_return')) %>%
      mutate(wt = (as.numeric(gsub('V','',variable))-1)/10) %>% select(-variable) %>% rename(pred = value)
    mix %<>% as.data.table
    mix =
      mix %>% group_by(date,wt) %>% arrange(desc(pred)) %>% slice(1:top_n) %>%
      summarise(ret = mean(target_1m_return)+1,.groups = 'keep') %>%
      group_by(wt) %>% arrange(date) %>%
      mutate(ret_roll = rollapply(ret,roll_period,prod, align = 'right', fill = NA)) %>%
      summarise(roll_avg = mean(ret_roll, na.rm = T),
                roll_min = min(ret_roll , na.rm = T), .groups = 'keep')

    best_wt = mix %>% filter(roll_avg == max(roll_avg)) %>% pull(wt)
    mix =
      mix %>% filter(wt %in% c(0,1)) %>% mutate(option = ifelse(wt == 0, 'target', 'index')) %>%
      bind_rows(mix %>% filter(wt %in% c(best_wt - 0.1, best_wt, best_wt + 0.1)) %>%
                  summarise(roll_avg = mean(roll_avg), roll_min = mean(roll_min)) %>%
                  mutate(wt = best_wt, option = 'best'))

    result = bind_rows(result,mix)
  }

  baselines = result %>% filter(option %in% c('target','index')) %>% group_by(option) %>% summarise_all(mean)
  mixed = result %>% filter(option %in% c('best')) %>% group_by(option) %>% summarise_all(mean)
  max_freq = result %>% filter(option %in% c('best')) %>% group_by(wt) %>% count() %>% ungroup() %>% arrange(desc(wt)) %>%filter(n == max(n)) %>% dplyr::slice(1) %>% pull(wt)
  ratio_sd = result %>% filter(option %in% c('best')) %>% pull(wt) %>% sd
  mixed %<>% mutate(max_freq = max_freq, ratio_sd = ratio_sd)
  final = bind_rows(baselines, mixed)

  return(final)

}
