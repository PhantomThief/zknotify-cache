zknotify-cache [![Build Status](https://travis-ci.org/PhantomThief/zknotify-cache.svg)](https://travis-ci.org/PhantomThief/zknotify-cache)
=======================

ZooKeeper通知更新的本地缓存

* 缓存按需延迟懒加载
* 通知加载可设置随机延迟，防止所有节点同时加载造成数据源瞬时压力过大
* 旧的缓存内容可定制清理回调
* 只支持Java8

## Get Started

* Stable版本
```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>zknotify-cache</artifactId>
    <version>0.1.7</version>
</dependency>
```

* Development版本
```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>zknotify-cache</artifactId>
    <version>0.1.8-SNAPSHOT</version>
</dependency>
```

```Java
ReloadableCache<List<String>> cache = ZkNotifyReloadCache.<List<String>> newBuilder() //
				.withCacheFactory(this::buildFromSource) // 配置cache构建方法，必须
				.withNotifyZkPath("/notifyPath1") // zk监听变更的路径，必须
				.withCuratorFactory(this::getCuratorFactory) // 提供zkClient的工场方法，必须
				.withMaxRandomSleepOnNotifyReload(30*1000) // zk通知reload时随机最大延迟时间，可选
				.withOldCleanup(this::cleanup) // 旧数据的清理方法，可选
				.enableAutoReload(1, TimeUnit.MINUTES) // 打开自动定时加载，可选
				.build();

List<String> content = cache.get(); // 获取内容，第一次调用本方法时才初始化

cache.reload(); // 通过zk通知所有节点更新
```

```Java
private List<String> buildFromSource() {
	// 从数据源加载数据
}

private CuratorFramework getCuratorFactory() {
	// 返回一个初始化好的CuratorFramework
}

private void cleanup(List<String> list) {
	// 清理旧对象操作
}
```