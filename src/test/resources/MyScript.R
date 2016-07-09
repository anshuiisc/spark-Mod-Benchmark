myAdd=function(x,y){
    sum=x+y
    return(sum)
}

data=function(){
prices <- c(132.45, 130.85, 130.00, 129.55, 130.85)
dates <- as.Date(c("2010-01-04", "2010-01-05", "2010-01-06","2010-01-07","2010-01-08"))
ibm.daily <- zoo(prices, dates)
print(ibm.daily)
}