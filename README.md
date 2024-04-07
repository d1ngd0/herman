# Herman

Herman (will be) a clustered stream engine for processing real-time data streams. Many of todays stream engines require the user to do one or many of the following:

- Pre-Configure all the stream aggregations they want up front
- Run aggregations on a single instance
- Understand the structure of their data
- Ensure their data has a specific schema it adheres to
- Chain together multiple stream engines to get the desired result

The goal of herman is to provide a stream engine that:

- Makes no assumptions about the structure of the data
- Can create aggregations across the cluster, yet return a single answer
- Is (potentially) preconfigured with data sources, but queries are run on an ad-hoc basis
- Abstract away many complexities of stream processing
- Allow users to interact through an intuitive SQL interface

As work progresses, this README will be updated with more information about the project. As of today, the project is in its infancy and is not yet ready for use.
