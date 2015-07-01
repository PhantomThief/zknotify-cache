zknotify-cache
=======================

ZooKeeper通知更新的本地缓存

* 缓存按需延迟懒加载
* 通知加载可设置随机延迟，防止所有节点同时加载造成数据源瞬时压力过大
* 旧的缓存内容可定制清理回调
* 只支持Java8

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>zknotify-cache</artifactId>
    <version>0.1.0</version>
</dependency>
```

```Java
ReloadableCache<List<String>> cache = ZkNotifyReloadCache.<List<String>> newBuilder() //
				.withCacheFactory(this::buildFromSource) //
				.withNotifyZkPath("/notifyPath1") //
				.withCuratorFactory(this::getCuratorFactory) //
				.withMaxRandomSleepOnNotifyReload(30*1000) //
				.withOldCleanup(this::cleanup) //
				.build();

List<String> content = cache.get();

cache.reload(); // notify all inited cache to reload
```

```Java
private List<String> buildFromSource() {
	// load data from data source
}

private CuratorFramework getCuratorFactory() {
	// return a started curator framework
}

private void cleanup(List<String> list) {
	// cleanup old resource
}
```