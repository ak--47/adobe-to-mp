# adobe-to-mp
 transform adobe data to mixpanel format and upload to cloud storage

key steps:

- [define lookups](https://github.com/ak--47/adobeToMixpanel/blob/main/index.js#L37-L56)
- [transform to JSON](https://github.com/ak--47/adobeToMixpanel/blob/main/index.js#L121-L134)
	- [custom transform](https://github.com/ak--47/adobeToMixpanel/blob/main/index.js#L185-L230)
- [transform to mixpanel](https://github.com/ak--47/adobeToMixpanel/blob/main/index.js#L167-L183)
- [upload to cloud storage](https://github.com/ak--47/adobeToMixpanel/blob/main/index.js#L148-L158)

some custom files are required which can be supplied by adobe:

```
.
├── guides
│   ├── evars.csv
│   ├── metrics.csv
│   └── props.csv
├── lookups-custom
│   ├── columns.csv
│   ├── eventList.csv
│   └── eventStandard.tsv
└── lookups-standard
    ├── browser.tsv
    ├── browser_type.tsv
    ├── color_depth.tsv
    ├── connection_type.tsv
    ├── country.tsv
    ├── javascript_version.tsv
    ├── languages.tsv
    ├── operating_systems.tsv
    ├── plugins.tsv
    ├── referrer_type.tsv
    ├── resolution.tsv
    └── search_engines.tsv
```