# Interpret Models and Results

### SHAP Single Model EDA
#' @export
eda_shap_train = function(shap, features){

  shap_mean = shap %>% group_by(date,variable) %>% dplyr::slice(1) %>% ungroup()

  shap_change = shap_mean %>% filter(date >= sort(unique(date),decreasing = T)[12]) %>%
    group_by(variable) %>%
    summarise(recent_shap1 = mean(mean_value)) %>% arrange(-recent_shap1) %>%
    mutate(`1y` = 1:nrow(.)) %>%
    left_join(shap_mean %>%
                filter(date %>% between(sort(unique(date),decreasing = T)[24],
                                        sort(unique(date),decreasing = T)[13])) %>%
                group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                rename(recent_shap2 = shap) %>% arrange(-recent_shap2) %>%
                mutate(`1y_2y` = 1:nrow(.)), by = 'variable') %>%
    left_join(shap_mean %>%
                filter(date %>% between(sort(unique(date),decreasing = T)[36],
                                        sort(unique(date),decreasing = T)[25])) %>%
                group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                rename(recent_shap3 = shap) %>% arrange(-recent_shap3) %>%
                mutate(`2y_3y` = 1:nrow(.)), by = 'variable') %>%
    select(variable, `1y`,`1y_2y`, `2y_3y`)

  shap_category = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>%
    group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    summarise(rank = round(mean(`1y`),0)) %>%
    mutate(category_rank = paste0(category, ": ",rank)) %>% arrange(rank) %>% select(category, category_rank)


  shap_category %<>% mutate(category_rank = factor(category_rank, levels = shap_category$category_rank))

  feat_lev = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
    rename(feature = variable) %>% arrange(`1y`) %>% pull(feature)

  shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    ungroup() %>%
    mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
    rename(feature = variable) %>%
    left_join(shap_category, by = 'category') %>%
    reshape2::melt(id.vars = c('feature', 'category', 'category_rank')) %>%
    mutate(variable = factor(variable, levels = c('2y_3y','1y_2y','1y'))) %>%
    ggplot(aes(x = factor(feature,levels = feat_lev), y = value, fill = variable)) +
    geom_bar(stat = 'identity', position = position_dodge(width = 0.7), width = 0.7, alpha = 0.8) +
    xlab('Variable') + ylab('Rank') +
    facet_wrap(vars(category_rank), scales = 'free' ,ncol = 2) +
    theme(strip.text.x = element_text(size = 15),
          axis.text = element_text(size = 12),
          axis.title = element_text(size = 15))
}


### SHAP Multi Model EDA
#' @export
eda_shap_test_mix = function(shap_ind_test, shap_10_test, ssl_ind, ssl_10, features, top_n = 30){

  shap_test_mix = ssl_ind %>% rename(pred_ind = pred_mean) %>%
    left_join(ssl_t10 %>% rename(pred_10 = pred_mean), by = c('date','stock_cd')) %>%
    mutate(pred_mix = (pred_ind+pred_10)/2) %>%
    group_by(date) %>% arrange(-pred_mix, .by_group = T) %>% dplyr::slice(1:top_n) %>%
    ungroup() %>% as.data.table

  shap_test_mix = shap_ind_test[,.(date,stock_cd,variable,shap_ind = value)][shap_test_mix, on = c('date','stock_cd')]
  shap_test_mix = shap_10_test[,.(date,stock_cd,variable,shap_10 = value)][shap_test_mix, on = c('date','stock_cd','variable')]
  weight_ref = shap_test_mix %>% group_by(date) %>% summarise(wt_ind = mean(pred_ind), wt_10 = mean(pred_10))

  shap_test_mix =
    shap_test_mix %>% left_join(weight_ref, by = 'date') %>%
    mutate(shap = shap_ind*wt_ind + shap_10*wt_10) %>%
    group_by(date,variable) %>% summarise(value = mean(abs(shap))) %>% ungroup()

  shap_change = shap_test_mix %>% filter(date >=  sort(unique(date),decreasing = T)[12]) %>%
    group_by(variable) %>% summarise(recent_shap1 = mean(value)) %>%
    arrange(-recent_shap1) %>% mutate(`1y` = 1:nrow(.)) %>%
    left_join(shap_test_mix %>%
                filter(date %>% between(sort(unique(date),decreasing = T)[24],
                                        sort(unique(date),decreasing = T)[13])) %>%
                group_by(variable) %>% summarise(shap = mean(value)) %>%
                rename(recent_shap2 = shap) %>% arrange(-recent_shap2) %>%
                mutate(`1y_2y` = 1:nrow(.)), by = 'variable') %>%
    left_join(shap_test_mix %>%
                filter(date %>% between(sort(unique(date),decreasing = T)[36],
                                        sort(unique(date),decreasing = T)[25])) %>%
                group_by(variable) %>% summarise(shap = mean(value)) %>%
                rename(recent_shap3 = shap) %>% arrange(-recent_shap3) %>%
                mutate(`2y_3y` = 1:nrow(.)), by = 'variable') %>%
    select(variable, `1y`,`1y_2y`, `2y_3y`)

  shap_category = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>%
    group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    summarise(rank = round(mean(`1y`),0)) %>%
    mutate(category_rank = paste0(category, ": ",rank)) %>% arrange(rank) %>% select(category, category_rank)

  shap_category %<>% mutate(category_rank = factor(category_rank, levels = shap_category$category_rank))

  feat_lev = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
    rename(feature = variable) %>% arrange(`1y`) %>% pull(feature)

  shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
    filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
    ungroup() %>%
    mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
    rename(feature = variable) %>%
    left_join(shap_category, by = 'category') %>%
    reshape2::melt(id.vars = c('feature', 'category', 'category_rank')) %>%
    mutate(variable = factor(variable, levels = c('2y_3y','1y_2y','1y'))) %>%
    ggplot(aes(x = factor(feature,levels = feat_lev), y = value, fill = variable)) +
    geom_bar(stat = 'identity', position = position_dodge(width = 0.7), width = 0.7, alpha = 0.8) +
    xlab('Variable') + ylab('Rank') +
    facet_wrap(vars(category_rank), scales = 'free' ,ncol = 2) +
    theme(strip.text.x = element_text(size = 15),
          axis.text = element_text(size = 12),
          axis.title = element_text(size = 15))
}


#' @export
eda_shap_newvars = function(shap_new, shap_old, features, years = '1y', scope = 'new'){

  newvars = setdiff(shap_new$variable %>% unique,shap_old$variable %>% unique)

  if(scope == 'new'){
    shap_mean = shap_new %>% group_by(date,variable) %>% dplyr::slice(1) %>% ungroup()

    shap_change = shap_mean %>% filter(date >= sort(unique(date),decreasing = T)[12]) %>%
      group_by(variable) %>%
      summarise(recent_shap1 = mean(mean_value)) %>% arrange(-recent_shap1) %>%
      mutate(`1y` = 1:nrow(.)) %>%
      left_join(shap_mean %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[24],
                                          sort(unique(date),decreasing = T)[13])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap2 = shap) %>% arrange(-recent_shap2) %>%
                  mutate(`1y_2y` = 1:nrow(.)), by = 'variable') %>%
      left_join(shap_mean %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[36],
                                          sort(unique(date),decreasing = T)[25])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap3 = shap) %>% arrange(-recent_shap3) %>%
                  mutate(`2y_3y` = 1:nrow(.)), by = 'variable') %>%
      select(variable, `1y`,`1y_2y`, `2y_3y`) %>%
      filter(variable %in% newvars)

    shap_category = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>%
      group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
      summarise(rank = round(mean(`1y`),0)) %>%
      mutate(category_rank = paste0(category, ": ",rank)) %>% arrange(rank) %>% select(category, category_rank)

    shap_category %<>% mutate(category_rank = factor(category_rank, levels = shap_category$category_rank))

    feat_lev = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
      mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
      rename(feature = variable) %>% arrange(`1y`) %>% pull(feature)

    shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>% group_by(category) %>% arrange(`1y`,.by_group = T) %>% dplyr::slice(1:5) %>%
      ungroup() %>%
      mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
      rename(feature = variable) %>%
      left_join(shap_category, by = 'category') %>%
      reshape2::melt(id.vars = c('feature', 'category', 'category_rank')) %>%
      mutate(variable = factor(variable, levels = c('2y_3y','1y_2y','1y'))) %>%
      ggplot(aes(x = factor(feature,levels = feat_lev), y = value, fill = variable)) +
      geom_bar(stat = 'identity', position = position_dodge(width = 0.7), width = 0.7, alpha = 0.8) +
      xlab('Variable') + ylab('Rank') +
      facet_wrap(vars(category_rank), scales = 'free' ,ncol = 2) +
      theme(strip.text.x = element_text(size = 15),
            axis.text = element_text(size = 12),
            axis.title = element_text(size = 15))
  } else if (scope == 'old'){

    shap_old_rank = shap_old %>% filter(date >= sort(unique(date),decreasing = T)[12]) %>%
      group_by(variable) %>%
      summarise(recent_shap1 = mean(mean_value)) %>% arrange(-recent_shap1) %>%
      mutate(`1y` = 1:nrow(.)) %>%
      left_join(shap_old %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[24],
                                          sort(unique(date),decreasing = T)[13])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap2 = shap) %>% arrange(-recent_shap2) %>%
                  mutate(`1y_2y` = 1:nrow(.)), by = 'variable') %>%
      left_join(shap_old %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[36],
                                          sort(unique(date),decreasing = T)[25])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap3 = shap) %>% arrange(-recent_shap3) %>%
                  mutate(`2y_3y` = 1:nrow(.)), by = 'variable') %>%
      select(variable, `1y`,`1y_2y`, `2y_3y`) %>%
      filter(!(variable %in% newvars))

    shap_new_rank = shap_new %>% filter(date >= sort(unique(date),decreasing = T)[12]) %>%
      group_by(variable) %>%
      summarise(recent_shap1 = mean(mean_value)) %>% arrange(-recent_shap1) %>%
      mutate(`1y` = 1:nrow(.)) %>%
      left_join(shap_new %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[24],
                                          sort(unique(date),decreasing = T)[13])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap2 = shap) %>% arrange(-recent_shap2) %>%
                  mutate(`1y_2y` = 1:nrow(.)), by = 'variable') %>%
      left_join(shap_new %>%
                  filter(date %>% between(sort(unique(date),decreasing = T)[36],
                                          sort(unique(date),decreasing = T)[25])) %>%
                  group_by(variable) %>% summarise(shap = mean(mean_value)) %>%
                  rename(recent_shap3 = shap) %>% arrange(-recent_shap3) %>%
                  mutate(`2y_3y` = 1:nrow(.)), by = 'variable') %>%
      select(variable, `1y`,`1y_2y`, `2y_3y`) %>%
      filter(!(variable %in% newvars))

    shap_change = shap_old_rank %>%
      left_join(shap_new_rank %>%
                  rename(new_1y = `1y`, new_1y_2y = `1y_2y`, new_2y_3y = `2y_3y`),
                by = 'variable') %>%
      mutate(diff_1y = new_1y/`1y`, diff_1y_2y = new_1y_2y/`1y_2y`, diff_2y_3y = new_2y_3y/`2y_3y`)

    shap_category = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>%
      group_by(category) %>% arrange(-get(paste0('diff_',years)),.by_group = T) %>% dplyr::slice(1:5) %>%
      summarise(rank = round(mean(get(years)),0)) %>%
      mutate(category_rank = paste0(category, ": ",rank)) %>% arrange(rank) %>% select(category, category_rank)

    shap_category %<>% mutate(category_rank = factor(category_rank, levels = shap_category$category_rank))

    feat_lev = shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>% group_by(category) %>% arrange(-get(paste0('diff_',years)),.by_group = T) %>% dplyr::slice(1:5) %>%
      mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
      rename(feature = variable) %>% pull(feature)

    shap_change %>% left_join(features %>% rename(variable = feature) %>% select(variable, category), by = 'variable') %>%
      filter(complete.cases(.)) %>% group_by(category) %>% arrange(-get(paste0('diff_',years)),.by_group = T) %>% dplyr::slice(1:5) %>%
      ungroup() %>%
      mutate(variable = str_wrap(gsub('_', ' ', variable),12)) %>%
      rename(feature = variable) %>%
      left_join(shap_category, by = 'category') %>%
      select('feature',years,paste0('new_',years),'category', 'category_rank') %>%
      reshape2::melt(id.vars = c('feature', 'category', 'category_rank')) %>%
      mutate(variable = factor(variable, levels = c(years,paste0('new_',years)))) %>%
      ggplot(aes(x = factor(feature,levels = feat_lev), y = value, fill = variable)) +
      geom_bar(stat = 'identity', position = position_dodge(width = 0.7), width = 0.7, alpha = 0.8) +
      xlab('Variable') + ylab('Rank') +
      facet_wrap(vars(category_rank), scales = 'free' ,ncol = 2) +
      theme(strip.text.x = element_text(size = 15),
            axis.text = element_text(size = 12),
            axis.title = element_text(size = 15))
  }

}


