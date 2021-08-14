# Util Funcs

# ssl_join

#' @export
ssl_join <- function(ssl1, ssl2, ssl1_ratio) {
  ssl_mix <-
    dplyr::inner_join(
      ssl1 %>% select(date, stock_cd, pred_mean, target_1m_return),
      ssl2 %>% select(date, stock_cd, pred_mean),
      by=c("date", "stock_cd")
    ) %>%
    rename(ssl1=pred_mean.x, ssl2=pred_mean.y) %>%
    mutate(pred_mix = ssl1*ssl1_ratio + ssl2*(1-ssl1_ratio)) %>%
    select(date, stock_cd, ssl1, ssl2, pred_mix, target_1m_return) %>%
    arrange(date) %>%
    group_by(date) %>%
    arrange(desc(pred_mix), .by_group = T) %>%
    ungroup()
  return(ssl_mix)
}

#' @export
upper_bound_calc = function(ssl1, ssl2, mix_ratio, top_n, first_bound=1.0, second_plus=0.05, num_tries) {
  
  if (num_tries <= 0) {
    stop("num_tries must be greater than 0")
  }
  if (first_bound < 0.1) {
    stop("first_bound must be greater than 0.1")
  }
  if (second_plus < 0.05) {
    stop("second_plus must be greater than 0.05")
  }
  
  first_upper_bound_vec = seq(0.1, first_bound, by=0.05)
  second_upper_plus_vec = seq(0.0, second_plus, by=0.05)
  bound_cumret_df <- data.frame()
  
  for (j in 1:num_tries) {
    
    print(j)
    
    ssl1_temp <- 
      ssl1 %>% 
      mutate(pred_sample_mean = rowMeans(ssl1 %>% select(sample(colnames(ssl1)[str_detect(colnames(ssl1), 'pred\\d')], 30, replace=T)))) %>% 
      select(date,stock_cd, pred_sample_mean)
    
    ssl2_temp <- 
      ssl2 %>% 
      mutate(pred_sample_mean = rowMeans(ssl2 %>% select(sample(colnames(ssl2)[str_detect(colnames(ssl2), 'pred\\d')], 30, replace=T)))) %>% 
      select(date,stock_cd, pred_sample_mean)
    
    ssl_mix <- 
      left_join(ssl1_temp, ssl2_temp, by = c("date", "stock_cd")) %>% 
      mutate(pred_mix = pred_sample_mean.x * mix_ratio + pred_sample_mean.y * (1-mix_ratio)) %>% 
      select(date, stock_cd, pred_mix) %>% 
      group_by(date) %>% 
      arrange(desc(pred_mix), .by_group=TRUE)
    
    rebalancing_dates = sort(unique(ssl_mix$date))
    
    for (first_upper_bound in first_upper_bound_vec) {
      for (second_upper_plus in second_upper_plus_vec) {
        
        second_upper_bound = first_upper_bound + second_upper_plus
        
        print(paste0("first_upper: ", first_upper_bound, "  /  second_upper: ", second_upper_bound))
        
        for(i in rebalancing_dates[-length(rebalancing_dates)]) {
          
          portfolio_return <-
            d_stock_price %>%
            select(-adj_open_price) %>% 
            filter(date >= i) %>%
            filter(date <= rebalancing_dates[which(rebalancing_dates==i)+1]) %>%
            filter(stock_cd %in% (ssl_mix %>% filter(date == i) %>% arrange(desc(pred_mix)) %>% dplyr::slice(1:top_n) %>% pull(stock_cd))) %>% 
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
  
  bound_cumret_df %>%
    filter(!substr(date, 1, 7) %in% c('2020-03', '2020-04')) %>% 
    group_by(try_idx, first_upper_bound, second_upper_bound) %>% 
    mutate(cumret = cumprod(portfolio_return+1)-1, 
           first_cnt = sum(first_cnt), 
           econd_cnt = sum(second_cnt)) %>% 
    filter(date == max(date)) %>% 
    mutate(cnt = ifelse(first_upper_bound == second_upper_bound, as.character(first_cnt), paste0(first_cnt, '→', second_cnt))) %>% 
    ggplot(aes(x=first_upper_bound, y=second_upper_bound, fill=cumret, label=cnt)) + 
    geom_tile() +
    geom_text(aes(x=first_upper_bound, y=second_upper_bound, label=cnt), color="black") + 
    facet_wrap(try_idx ~ .) +
    theme_minimal()
}
