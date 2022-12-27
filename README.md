template for abstraction over gorm library
including several functions

1) error processing  
 - internal errors (meant errors whose caused by developer mistake) will be logged and replaced by common error, which can be directly passed to ended user
 - not found error just an alias for gorm not found error, can be places on tier near the internal error

2) extended logs
 - add error tracing w/o system calls. only trace thorough you own project
 - add all parameters whose were passed to builder in conditions

3) elaborated preload calling order (alpha)
gorm doesn't support queries like:
```
query := db.Preload("Role")
err := query.Count(&count)
err := query.Find(&response)
```
in case if you for some reason want to specify preloads earlier than counting - this interface will store preload conditions in .preloads field 
and will apply preloads before query closures    