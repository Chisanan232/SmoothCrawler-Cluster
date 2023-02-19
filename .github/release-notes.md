## Refactoring the core module *crawler* to be sub-package.

### 🎉🎊🍾 New feature
<hr>

#### Source Code

1. Integrate all the Zookeeper node paths into new object **ZookeeperPath**
2. Properties in module *crawler.crawlers*:

  * *register*
  * *dispatcher*

3. Module *register* for processing all meta-data registrations of cluster
4. Module *crawler.attributes* for generating some basic crawler's attributes
5. Module *crawler.dispatcher* who dispatches which workflow crawler should call to use
6. Module *crawler.workflow* about implementing every crawler role's job details
7. Module *model._data* for the inner usages about data objects
8. Content for the new modules or objects in package documentation

#### Test

1. Add more tests to cover more features in source code
2. Unit test for new module
3. Integration test for new module

#### Documentation

1. New content for the new modules or objects
2. Software architectures of the new modules in sub-package *_crawler_*


### 🔧⚙️🔩 Breaking Change
<hr>

#### Source Code

1. Add one more condition of major function *ZookeeperCrawler.wait_for_task* about it won't run anymore if crawler role
  is **CrawlerStateRole.DEAD_RUNNER**


### 🔬🧪🧬 Refactor
<hr>

#### Source Code

1. Integrate all operating functions of getting or setting values with Zookeeper as 2 protected util functions

  * *ZookeeperCrawler._metadata_util.get_metadata_from_zookeeper* -> *ZookeeperCrawler._get_metadata*
  * *ZookeeperCrawler._metadata_util.set_metadata_to_zookeeper* -> *ZookeeperCrawler._set_metadata*

2. Refactoring the *crawler* module to be *_crawler_* sub-package
3. Refactoring the *crawler.crawlers* module implementation:

  * Change to use *crawler.attributes* module to generate and manage some basic crawler's attributes, e.g., name, identity, etc
  * Change the return data to be **CrawlerName** at property *name*
  * Change to use *register* to process all meta-data registrations
  * All crawler role's job details move to new module *crawler.workflow*
  * Use *crawler.dispatcher* to dispatch needed workflow to run current crawler role's job for crawler
