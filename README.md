# cbackend

Notes on libpostal for parsing/normalizing addresses:

[At the moment, libpostal does not attempt to resolve ambiguities in addresses, and often produces multiple potential expansions. Some may be nonsensical (“Main St” expands to both “Main Street” and “Main Saint”), but the correct form will be among them. The outputs of libpostal’s expand_address can be treated as a set and address matching can be seen as a doing a set intersection, or a JOIN in SQL parlance. In the search setting, one should index all of the strings produced, and use the same code to normalize user queries before sending them to the search server/database.](https://medium.com/@albarrentine/statistical-nlp-on-openstreetmap-b9d573e6cc86#.3k21scg7o)