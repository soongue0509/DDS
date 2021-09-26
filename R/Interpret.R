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

#' @export
shap_viz <- function(test_shap) {
  
  StatSina <- ggproto('StatSina', Stat, required_aes = c('x', 'y'),
                      setup_data = function(data, params) {
                        if (is.double(data$x) && !.has_groups(data) && any(data$x != data$x[1L])) {
                          stop('Continuous x aesthetic -- did you forget aes(group=...)?',
                               call. = FALSE
                          )
                        }
                        
                        data
                      },
                      
                      setup_params = function(data, params) {
                        params$maxwidth <- params$maxwidth %||% (resolution(data$x %||% 0) * 0.9)
                        
                        if (is.null(params$binwidth) && is.null(params$bins)) {
                          params$bins <- 50
                        }
                        
                        params
                      },
                      
                      compute_panel = function(self, data, scales, scale = TRUE, method = 'density',
                                               bw = 'nrd0', kernel = 'gaussian', binwidth = NULL,
                                               bins = NULL, maxwidth = 1, adjust = 1, bin_limit = 1,
                                               seed = NA) {
                        
                        if (!is.null(binwidth)) {
                          bins <- bin_breaks_width(scales$y$dimension() + 1e-8, binwidth)
                        } else {
                          bins <- bin_breaks_bins(scales$y$dimension() + 1e-8, bins)
                        }
                        
                        data <- ggproto_parent(Stat, self)$compute_panel(data, scales,
                                                                         scale = scale, method = method, bw = bw, kernel = kernel,
                                                                         bins = bins$breaks, maxwidth = maxwidth, adjust = adjust,
                                                                         bin_limit = bin_limit)
                        
                        if (is.logical(scale)) {
                          scale <- if (scale) 'area' else 'width'
                        }
                        # choose how sinas are scaled relative to each other
                        data$sinawidth <- switch(
                          scale,
                          # area : keep the original densities but scale them to a max width of 1
                          #        for plotting purposes only
                          area = data$density / max(data$density),
                          # count: use the original densities scaled to a maximum of 1 (as above)
                          #        and then scale them according to the number of observations
                          count = data$density / max(data$density) * data$n / max(data$n),
                          # width: constant width (each density scaled to a maximum of 1)
                          width = data$scaled
                        )
                        
                        if (!is.na(seed)) {
                          new_seed <- sample(.Machine$integer.max, 1L)
                          set.seed(seed)
                          on.exit(set.seed(new_seed))
                        }
                        data$xmin <- data$x - maxwidth / 2
                        data$xmax <- data$x + maxwidth / 2
                        data$x_diff <- runif(nrow(data), min = -1, max = 1) *
                          maxwidth * data$sinawidth/2
                        data$width <- maxwidth
                        
                        # jitter y values if the input is input is integer
                        if (all(data$y == floor(data$y))) {
                          data$y <- jitter(data$y)
                        }
                        
                        data
                      },
                      
                      compute_group = function(data, scales, scale = TRUE, method = 'density',
                                               bw = 'nrd0', kernel = 'gaussian',
                                               maxwidth = 1, adjust = 1, bin_limit = 1,
                                               bins = NULL) {
                        if (nrow(data) == 0) return(NULL)
                        
                        if (nrow(data) < 3) {
                          data$density <- 0
                          data$scaled <- 1
                        } else if (method == 'density') { # density kernel estimation
                          range <- range(data$y, na.rm = TRUE)
                          bw <- calc_bw(data$y, bw)
                          dens <- compute_density(data$y, data$w, from = range[1], to = range[2],
                                                  bw = bw, adjust = adjust, kernel = kernel)
                          densf <- stats::approxfun(dens$x, dens$density, rule = 2)
                          
                          data$density <- densf(data$y)
                          data$scaled <- data$density / max(dens$density)
                          
                          data
                        } else { # bin based estimation
                          bin_index <- cut(data$y, bins, include.lowest = TRUE, labels = FALSE)
                          data$density <- tapply(bin_index, bin_index, length)[as.character(bin_index)]
                          data$density[data$density <= bin_limit] <- 0
                          data$scaled <- data$density / max(data$density)
                        }
                        
                        # Compute width if x has multiple values
                        if (length(unique(data$x)) > 1) {
                          width <- diff(range(data$x)) * maxwidth
                        } else {
                          width <- maxwidth
                        }
                        data$width <- width
                        data$n <- nrow(data)
                        data$x <- mean(range(data$x))
                        data
                      },
                      finish_layer = function(data, params) {
                        # rescale x in case positions have been adjusted
                        x_mod <- (data$xmax - data$xmin) / data$width
                        data$x <- data$x + data$x_diff * x_mod
                        data
                      },
                      extra_params = 'na.rm'
  )
  
  stat_sina <- function(mapping = NULL, data = NULL, geom = 'sina',
                        position = 'dodge', scale = 'area', method = 'density',
                        bw = 'nrd0', kernel = 'gaussian', maxwidth = NULL,
                        adjust = 1, bin_limit = 1, binwidth = NULL, bins = NULL,
                        seed = NA, ..., na.rm = FALSE, show.legend = NA,
                        inherit.aes = TRUE) {
    method <- match.arg(method, c('density', 'counts'))
    
    layer(
      data = data,
      mapping = mapping,
      stat = StatSina,
      geom = geom,
      position = position,
      show.legend = show.legend,
      inherit.aes = inherit.aes,
      params = list(scale = scale, method = method, bw = bw, kernel = kernel,
                    maxwidth = maxwidth, adjust = adjust, bin_limit = bin_limit,
                    binwidth = binwidth, bins = bins, seed = seed, na.rm = na.rm, ...)
    )
  }
  
  geom_sina <- function(mapping = NULL, data = NULL,
                        stat = 'sina', position = 'dodge',
                        ...,
                        na.rm = FALSE,
                        show.legend = NA,
                        inherit.aes = TRUE) {
    layer(
      data = data,
      mapping = mapping,
      stat = stat,
      geom = GeomPoint,
      position = position,
      show.legend = show.legend,
      inherit.aes = inherit.aes,
      params = list(
        na.rm = na.rm,
        ...
      )
    )
  }
  
  bins <- function(breaks, closed = c('right', 'left'),
                   fuzz = 1e-08 * stats::median(diff(breaks))) {
    stopifnot(is.numeric(breaks))
    closed <- match.arg(closed)
    
    breaks <- sort(breaks)
    # Adapted base::hist - this protects from floating point rounding errors
    if (closed == 'right') {
      fuzzes <- c(-fuzz, rep.int(fuzz, length(breaks) - 1))
    } else {
      fuzzes <- c(rep.int(-fuzz, length(breaks) - 1), fuzz)
    }
    
    structure(
      list(
        breaks = breaks,
        fuzzy = breaks + fuzzes,
        right_closed = closed == 'right'
      ),
      class = 'ggplot2_bins'
    )
  }
  
  compute_density <- function(x, w, from, to, bw = "nrd0", adjust = 1,
                              kernel = "gaussian", n = 512) {
    nx <- length(x)
    if (is.null(w)) {
      w <- rep(1 / nx, nx)
    }
    
    # if less than 2 points return data frame of NAs and a warning
    if (nx < 2) {
      warning("Groups with fewer than two data points have been dropped.", call. = FALSE)
      return(new_data_frame(list(
        x = NA_real_,
        density = NA_real_,
        scaled = NA_real_,
        ndensity = NA_real_,
        count = NA_real_,
        n = NA_integer_
      ), n = 1))
    }
    
    dens <- stats::density(x, weights = w, bw = bw, adjust = adjust,
                           kernel = kernel, n = n, from = from, to = to)
    
    new_data_frame(list(
      x = dens$x,
      density = dens$y,
      scaled =  dens$y / max(dens$y, na.rm = TRUE),
      ndensity = dens$y / max(dens$y, na.rm = TRUE),
      count =   dens$y * nx,
      n = nx
    ), n = length(dens$x))
  }
  calc_bw <- function(x, bw) {
    if (is.character(bw)) {
      if (length(x) < 2)
        stop("need at least 2 points to select a bandwidth automatically", call. = FALSE)
      bw <- switch(
        base::tolower(bw),
        nrd0 = stats::bw.nrd0(x),
        nrd = stats::bw.nrd(x),
        ucv = stats::bw.ucv(x),
        bcv = stats::bw.bcv(x),
        sj = ,
        `sj-ste` = stats::bw.SJ(x, method = "ste"),
        `sj-dpi` = stats::bw.SJ(x, method = "dpi"),
        stop("unknown bandwidth rule")
      )
    }
    bw
  }
  
  bin_breaks <- function(breaks, closed = c('right', 'left')) {
    bins(breaks, closed)
  }
  
  bin_breaks_width <- function(x_range, width = NULL, center = NULL,
                               boundary = NULL, closed = c('right', 'left')) {
    stopifnot(length(x_range) == 2)
    
    # if (length(x_range) == 0) {
    #   return(bin_params(numeric()))
    # }
    stopifnot(is.numeric(width), length(width) == 1)
    if (width <= 0) {
      stop('`binwidth` must be positive', call. = FALSE)
    }
    
    if (!is.null(boundary) && !is.null(center)) {
      stop('Only one of \'boundary\' and \'center\' may be specified.')
    } else if (is.null(boundary)) {
      if (is.null(center)) {
        # If neither edge nor center given, compute both using tile layer's
        # algorithm. This puts min and max of data in outer half of their bins.
        boundary <- width / 2
      } else {
        # If center given but not boundary, compute boundary.
        boundary <- center - width / 2
      }
    }
    
    # Find the left side of left-most bin: inputs could be Dates or POSIXct, so
    # coerce to numeric first.
    x_range <- as.numeric(x_range)
    width <- as.numeric(width)
    boundary <- as.numeric(boundary)
    shift <- floor((x_range[1] - boundary) / width)
    origin <- boundary + shift * width
    
    # Small correction factor so that we don't get an extra bin when, for
    # example, origin = 0, max(x) = 20, width = 10.
    max_x <- x_range[2] + (1 - 1e-08) * width
    breaks <- seq(origin, max_x, width)
    
    bin_breaks(breaks, closed = closed)
  }
  
  bin_breaks_bins <- function(x_range, bins = 30, center = NULL,
                              boundary = NULL, closed = c('right', 'left')) {
    stopifnot(length(x_range) == 2)
    
    bins <- as.integer(bins)
    if (bins < 1) {
      stop('Need at least one bin.', call. = FALSE)
    } else if (bins == 1) {
      width <- diff(x_range)
      boundary <- x_range[1]
    } else {
      width <- (x_range[2] - x_range[1]) / (bins - 1)
    }
    
    bin_breaks_width(x_range, width,
                     boundary = boundary, center = center,
                     closed = closed
    )
  }
  
  feature_list <- 
    dbConnect(
      MySQL(),
      user = 'betterlife',
      password = 'snail132',
      host = 'betterlife.duckdns.org',
      port = 1231 ,
      dbname = 'stock_db') %>% dbGetQuery("select * from feature_list_20210709")
  
  #label_format = "%.1e"
  label_format = "%.3f"
  
  data_temp = 
    test_shap %>% 
    # 1. 상위 N개 종목만 추출
    inner_join(
      test_shap %>% 
        select(date, stock_cd, pred_mean) %>%
        unique() %>% 
        group_by(date) %>% 
        arrange(desc(pred_mean), .by_group=T) %>% 
        dplyr::slice(1:50) %>% 
        ungroup() %>% 
        select(date, stock_cd),
      by=c("date", "stock_cd")
    ) %>% 
    # 2. 매크로 변수 제외
    left_join(feature_list %>% filter(category == 'Macro') %>% select(feature, category), by=c("variable"="feature")) %>% 
    filter(is.na(category)) %>% 
    select(-category) %>% 
    # 3. 연도별 주요 변수
    mutate(yyyy = substr(date, 1, 4)) %>% 
    select(-stock_cd, -pred_mean, -date) %>% 
    group_by(yyyy, variable) %>% 
    mutate(mean_value = mean(mean_value)) %>% 
    ungroup()
  
  data_long =
    data_temp %>% 
    inner_join(
      data_temp %>% 
        select(yyyy, variable, mean_value) %>% 
        unique() %>% 
        group_by(yyyy) %>% 
        arrange(desc(mean_value), .by_group=T) %>% 
        dplyr::slice(1:10) %>% 
        mutate(variable2 = factor(paste0(yyyy,'_',str_pad(n():1, width=2, pad=0, side="left"),'_',variable))) %>% 
        select(-mean_value),
      by=c("yyyy", "variable")
    ) %>% 
    as.data.table() %>% 
    arrange(yyyy, desc(mean_value))
  
  x_bound <- max(abs(data_long$value))*1.1
  plot1 <- 
    ggplot(data = data_long) +
    coord_flip(ylim = c(-x_bound, x_bound)) +
    geom_hline(yintercept = 0) + # the y-axis beneath
    geom_sina(aes(x = variable2, y = value, color = stdfvalue),
              method = "counts", maxwidth = 0.7, alpha = 0.7) +
    # print the mean absolute value:
    geom_text(data = unique(data_long[, c("variable2", "mean_value", "yyyy")]),
              aes(x = variable2, y=-Inf, label = sprintf(label_format, mean_value)),
              size = 3, alpha = 0.7,
              hjust = -0.2,
              fontface = "bold",
              check_overlap = TRUE) +
    scale_color_gradient(low="#0389FA", high="#FF0152",
                         breaks=c(0,1), labels=c(" Low", "High "),
                         guide = guide_colorbar(barwidth = 12, barheight = 0.3)) +
    theme_bw() + 
    theme(axis.line.y = element_blank(),
          axis.ticks.y = element_blank(), # remove axis line
          legend.position="bottom",
          legend.title=element_text(size=10),
          legend.text=element_text(size=8),
          axis.title.x= element_text(size = 10)) + 
    labs(y = "SHAP value (impact on model output)", x = "", color = "Feature value  ") +
    facet_wrap(~yyyy, scale="free_y") +
    scale_x_discrete(labels = function(x) str_replace(x, "\\d{4}_\\d{2}_", ""))
  return(plot1)
}
