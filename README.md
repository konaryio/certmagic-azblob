## Certmagic AZBlob

Caddy storage for Azure Blob Storage. 

Right now this solution uses container level locks instead of blob level locks. The upside is it is easier to manage the lease and there is only ever 1 lock that needs managed. 

This could be converted to per blob locks in the future if the need arises.