package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	RedisAddr     = "192.168.140.128:6379"
	RedisPassword = ""
	RedisDB       = 0
)

// 检查redisClient是否实现了RedisClient的全部接口
var _ RedisClient = (*redisClient)(nil)

type RedisClient interface {
	// Set 设置键值对
	Set(key, value string, expiration time.Duration) error
	// Get 获取键的值
	Get(key string) (string, error)
	// Delete 删除键
	Delete(key string) error
	// Exists 检查键是否存在
	Exists(key string) (bool, error)
	// SetWithExpire 设置带过期时间的键值对
	SetWithExpire(key, value string, expiration time.Duration) error
	// Increment 对数字值进行递增
	Increment(key string) (int64, error)
	// ListRPush 从右侧推入列表元素
	ListRPush(key string, values ...interface{}) error
	// ListLLen 获取列表长度
	ListLLen(key string) (int64, error)
	// ListLPop 从左侧弹出列表元素
	ListLPop(key string) (string, error)
	// ListLRange 获取列表指定范围的元素
	ListLRange(key string, start, stop int64) ([]string, error)
	// SetSAdd 添加元素到集合
	SetSAdd(key string, members ...interface{}) error
	// SetSRem 移除集合中的元素
	SetSRem(key string, members ...interface{}) error
	// SetSMembers 获取集合所有元素
	SetSMembers(key string) ([]string, error)
	// SetSIsMember 检查元素是否在集合中
	SetSIsMember(key string, member interface{}) (bool, error)
	// SetSCard 获取集合元素数量
	SetSCard(key string) (int64, error)
	// SetSRandMember 随机获取集合中的一个元素
	SetSRandMember(key string) (string, error)
	// SetZAdd 添加/更新有序集合中的元素（带分数）
	SetZAdd(key string, members ...redis.Z) error
	// SetZRem 移除有序集合中的元素
	SetZRem(key string, members ...interface{}) error
	// SetZRange 获取有序集合指定范围的元素(按分数升序)
	SetZRange(key string, start, stop int64) ([]string, error)
	// SetZRevRange 获取有序集合指定范围的元素(按分数降序)
	SetZRevRange(key string, start, stop int64) ([]string, error)
	// SetZCard 获取有序集合元素数量
	SetZCard(key string) (int64, error)
	// SetZRangeByScore 获取有序集合指定分数范围内的元素(按分数升序)
	SetZRangeByScore(key string, min, max string, start, stop int64) ([]string, error)
	// SetZRevRangeByScore 获取有序集合指定分数范围内的元素(按分数降序)
	SetZRevRangeByScore(key string, min, max string, start, stop int64) ([]string, error)
	// SetZScore 获取有序集合中元素的分数
	SetZScore(key string, member string) error
	// SetZIncrBy 增加有序集合中元素的分数
	SetZIncrBy(key string, member string, increment float64) error
	// SetZRank 获取有序集合中元素的排名（按分数升序）
	SetZRank(key string, member string) error
	// SetZRevRank 获取有序集合中元素的排名（按分数降序）
	SetZRevRank(key string, member string) error
	// SetHashSet 设置哈希字段
	HashSet(hashKey string, values ...interface{}) error
	// SetHashGetAll 获取哈希字段的所有值
	HashGetAll(hashKey string) (map[string]string, error)
	// SetHashGet 获取哈希字段的值
	HashGet(hashKey string, field string) (string, error)
	// Close 关闭Redis连接
	Close()
}

// redisClient 封装Redis客户端
type redisClient struct {
	client *redis.Client
	ctx    context.Context
}

type RedisConfig struct {
	Addr         string // Redis地址，格式为"host:port"
	Password     string // Redis密码
	DB           int    // Redis数据库索引
	PoolSize     int    // 连接池大小
	MinIdleConns int    // 最小空闲连接数
	MaxRetries   int    // 最大重试次数
}

// NewRedisClient 创建Redis客户端实例
func NewRedisClient(config *RedisConfig, ctx context.Context) (*redisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 需5s内连接成功，否则报错
	_, err := client.Ping(timeoutCtx).Result()
	if err != nil {
		return nil, fmt.Errorf("无法连接到Redis: %v", err)
	}

	log.Println("成功连接到Redis")
	return &redisClient{
		client: client,
		ctx:    ctx,
	}, nil
}

// Set 设置键值对
func (rc *redisClient) Set(key, value string, expiration time.Duration) error {
	err := rc.client.Set(rc.ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("设置键值对失败: %v", err)
	}
	log.Printf("设置成功: %s -> %s", key, value)
	return nil
}

// Get 获取键的值
func (rc *redisClient) Get(key string) (string, error) {
	value, err := rc.client.Get(rc.ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("键不存在: %s", key)
	} else if err != nil {
		return "", fmt.Errorf("获取键值失败: %v", err)
	}
	log.Printf("获取成功: %s -> %s", key, value)
	return value, nil
}

// Delete 删除键
func (rc *redisClient) Delete(key string) error {
	err := rc.client.Del(rc.ctx, key).Err()
	if err != nil {
		return fmt.Errorf("删除键失败: %v", err)
	}
	log.Printf("删除成功: %s", key)
	return nil
}

// Exists 检查键是否存在
func (rc *redisClient) Exists(key string) (bool, error) {
	result, err := rc.client.Exists(rc.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("检查键存在失败: %v", err)
	}
	exists := result > 0
	log.Printf("键 %s 存在: %v, result: %v", key, exists, result)
	return exists, nil
}

// SetWithExpire 设置带过期时间的键值对
func (rc *redisClient) SetWithExpire(key, value string, expiration time.Duration) error {
	err := rc.client.SetEx(rc.ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("设置带过期时间的键值对失败: %v", err)
	}
	log.Printf("设置带过期时间成功: %s -> %s (过期时间: %v)", key, value, expiration)
	return nil
}

// Increment 对数字值进行递增
func (rc *redisClient) Increment(key string) (int64, error) {
	result, err := rc.client.Incr(rc.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("递增操作失败: %v", err)
	}
	log.Printf("递增成功: %s -> %d", key, result)
	return result, nil
}

// ListRPush 从右侧推入列表元素
func (rc *redisClient) ListRPush(key string, values ...interface{}) error {
	err := rc.client.RPush(rc.ctx, key, values...).Err()
	if err != nil {
		return fmt.Errorf("推入列表元素失败: %v", err)
	}
	log.Printf("列表元素推入成功: %s -> %v", key, values)

	return nil
}

// ListLLen 获取列表长度
func (rc *redisClient) ListLLen(key string) (int64, error) {
	length, err := rc.client.LLen(rc.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取列表长度失败: %v", err)
	}
	log.Printf("列表长度: %d", length)
	return length, nil
}

// ListLPop 从左侧弹出列表元素
func (rc *redisClient) ListLPop(key string) (string, error) {
	value, err := rc.client.LPop(rc.ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("列表 %s 为空", key)
	} else if err != nil {
		return "", fmt.Errorf("弹出列表元素失败: %v", err)
	}
	log.Printf("列表元素弹出成功: %s -> %s", key, value)
	return value, nil
}

// ListLRange 获取列表指定范围的元素[start, stop]
func (rc *redisClient) ListLRange(key string, start, stop int64) ([]string, error) {
	items, err := rc.client.LRange(rc.ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("获取列表元素失败: %v", err)
	}
	log.Printf("列表元素: %v", items)
	return items, nil
}

// SetSAdd 添加元素到集合
func (rc *redisClient) SetSAdd(key string, members ...interface{}) error {
	err := rc.client.SAdd(rc.ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("添加集合元素失败: %v", err)
	}
	log.Printf("集合元素添加成功: %s -> %v", key, members)
	return nil
}

// SetSRem 移除集合中的元素
func (rc *redisClient) SetSRem(key string, members ...interface{}) error {
	err := rc.client.SRem(rc.ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("移除集合元素失败: %v", err)
	}
	log.Printf("集合元素移除成功: %s -> %v", key, members)
	return nil
}

// SetSMembers 获取集合所有元素
func (rc *redisClient) SetSMembers(key string) ([]string, error) {
	members, err := rc.client.SMembers(rc.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("获取集合元素失败: %v", err)
	}
	log.Printf("集合所有元素: %v", members)
	return members, nil
}

// SetSIsMember 检查元素是否在集合中
func (rc *redisClient) SetSIsMember(key string, member interface{}) (bool, error) {
	isMember, err := rc.client.SIsMember(rc.ctx, key, member).Result()
	if err != nil {
		return false, fmt.Errorf("检查集合元素失败: %v", err)
	}
	log.Printf("元素 %v 是否在集合 %s 中: %t", member, key, isMember)
	return isMember, nil
}

// SetSCard 获取集合元素数量
func (rc *redisClient) SetSCard(key string) (int64, error) {
	cardinality, err := rc.client.SCard(rc.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取集合元素数量失败: %v", err)
	}
	log.Printf("集合元素数量: %d", cardinality)
	return cardinality, nil
}

// SetSRandMember 随机获取集合中的一个元素
func (rc *redisClient) SetSRandMember(key string) (string, error) {
	randomMember, err := rc.client.SRandMember(rc.ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("随机获取集合元素失败: %v", err)
	}
	log.Printf("随机获取的元素: %s", randomMember)
	return randomMember, nil
}

// SetZAdd 添加/更新有序集合中的元素（带分数）
func (rc *redisClient) SetZAdd(key string, members ...redis.Z) error {
	err := rc.client.ZAdd(rc.ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("添加/更新有序集合元素失败: %v", err)
	}
	log.Printf("有序集合元素添加/更新成功: %s -> %v", key, members)
	return nil
}

// SetZRem 移除有序集合中的元素
func (rc *redisClient) SetZRem(key string, members ...interface{}) error {
	err := rc.client.ZRem(rc.ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("移除有序集合元素失败: %v", err)
	}
	log.Printf("有序集合元素移除成功: %s -> %v", key, members)
	return nil
}

// SetZRange 获取有序集合指定范围的元素(按分数升序) [start, stop]
func (rc *redisClient) SetZRange(key string, start, stop int64) ([]string, error) {
	members, err := rc.client.ZRange(rc.ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合元素失败: %v", err)
	}
	log.Printf("有序集合所有元素（按分数升序）: %v", members)
	return members, nil
}

// SetZRevRange 获取有序集合指定范围的元素(按分数降序) [start, stop]
func (rc *redisClient) SetZRevRange(key string, start, stop int64) ([]string, error) {
	members, err := rc.client.ZRevRange(rc.ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合元素失败: %v", err)
	}
	log.Printf("有序集合所有元素（按分数降序）: %v", members)
	return members, nil
}

// SetZCard 获取有序集合元素数量
func (rc *redisClient) SetZCard(key string) (int64, error) {
	cardinality, err := rc.client.ZCard(rc.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取有序集合元素数量失败: %v", err)
	}
	log.Printf("有序集合元素数量: %d", cardinality)
	return cardinality, nil
}

// SetZRangeByScore 获取有序集合指定分数范围内的元素(按分数升序) [min, max] [start, stop]
func (rc *redisClient) SetZRangeByScore(key string, min, max string, start, stop int64) ([]string, error) {
	if min > max {
		return nil, fmt.Errorf("min 必须小于等于 max")
	}

	members, err := rc.client.ZRangeByScore(rc.ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: start,
		Count:  stop - start + 1,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合元素失败: %v", err)
	}
	log.Printf("有序集合，在 %s 到 %s 分数，%d 到 %d 范围内的所有元素（按分数升序）: %v", min, max, start, stop, members)
	return members, nil
}

// SetZRevRangeByScore 获取有序集合指定分数范围内的元素(按分数降序) [min, max] [start, stop]
func (rc *redisClient) SetZRevRangeByScore(key string, min, max string, start, stop int64) ([]string, error) {
	if min > max {
		return nil, fmt.Errorf("min 必须小于等于 max")
	}

	members, err := rc.client.ZRevRangeByScore(rc.ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: start,
		Count:  stop - start + 1,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合元素失败: %v", err)
	}
	log.Printf("有序集合，在 %s 到 %s 分数，%d 到 %d 范围内的所有元素（按分数降序）: %v", min, max, start, stop, members)
	return members, nil
}

// SetZScore 获取有序集合中元素的分数
func (rc *redisClient) SetZScore(key string, member string) error {
	score, err := rc.client.ZScore(rc.ctx, key, member).Result()
	if err != nil {
		return fmt.Errorf("获取元素分数失败: %v", err)
	}
	log.Printf("元素 %s 的分数为 %f", member, score)
	return nil
}

// SetZIncrBy 增加有序集合中元素的分数
func (rc *redisClient) SetZIncrBy(key string, member string, increment float64) error {
	newScore, err := rc.client.ZIncrBy(rc.ctx, key, increment, member).Result()
	if err != nil {
		return fmt.Errorf("增加元素分数失败: %v", err)
	}
	log.Printf("元素 %s 的分数增加为 %f", member, newScore)
	return nil
}

// SetZRank 获取有序集合中元素的排名（按分数升序）
func (rc *redisClient) SetZRank(key string, member string) error {
	rank, err := rc.client.ZRank(rc.ctx, key, member).Result()
	if err != nil {
		return fmt.Errorf("获取元素排名失败: %v", err)
	}
	log.Printf("元素 %s 的排名为 %d(按分数升序)", member, rank)
	return nil
}

// SetZRevRank 获取有序集合中元素的排名（按分数降序）
func (rc *redisClient) SetZRevRank(key string, member string) error {
	rank, err := rc.client.ZRevRank(rc.ctx, key, member).Result()
	if err != nil {
		return fmt.Errorf("获取元素排名失败: %v", err)
	}
	log.Printf("元素 %s 的排名为 %d(按分数降序)", member, rank)
	return nil
}

// SetHashSet 设置哈希字段
func (rc *redisClient) HashSet(hashKey string, values ...interface{}) error {
	err := rc.client.HSet(rc.ctx, hashKey, values...).Err()
	if err != nil {
		return fmt.Errorf("设置哈希字段失败: %v", err)
	}
	log.Printf("哈希字段 %s 设置成功: %v", hashKey, values)
	return nil
}

// SetHashGetAll 获取哈希字段的所有值
func (rc *redisClient) HashGetAll(hashKey string) (map[string]string, error) {
	fields, err := rc.client.HGetAll(rc.ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("获取哈希字段失败: %v", err)
	}
	log.Printf("哈希字段: %v", fields)
	return fields, nil
}

// SetHashGet 获取哈希字段的值
func (rc *redisClient) HashGet(hashKey string, field string) (string, error) {
	value, err := rc.client.HGet(rc.ctx, hashKey, field).Result()
	if err != nil {
		return "", fmt.Errorf("获取哈希字段失败: %v", err)
	}
	log.Printf("哈希字段 %s 的值为 %s", field, value)
	return value, nil
}

// Close 关闭Redis连接
func (rc *redisClient) Close() {
	if rc.client != nil {
		rc.client.Close()
		log.Println("Redis连接已关闭")
	}
}

func GetRedisClient(rc *redisClient) *redisClient {
	return rc
}

func main() {
	// 创建Redis客户端

	// 请根据您的Redis配置修改以下参数
	redisClient, err := NewRedisClient(&RedisConfig{
		Addr:         RedisAddr,
		Password:     RedisPassword,
		DB:           RedisDB,
		PoolSize:     100,
		MinIdleConns: 10,
		MaxRetries:   3,
	}, context.Background())
	if err != nil {
		log.Fatalf("创建Redis客户端失败: %v", err)
	}

	defer redisClient.Close()

	fmt.Println("\n=== Redis基础操作演示 ===")

	// 1. 设置和获取键值对
	fmt.Println("1. 设置和获取键值对:")
	redisClient.Set("greeting", "Hello, Redis!!!", 0)
	redisClient.Get("greeting")

	// 2. 设置带过期时间的键值对
	fmt.Println("\n2. 设置带过期时间的键值对:")
	redisClient.SetWithExpire("temp_key", "临时数据", 30*time.Second)
	redisClient.Get("temp_key")

	// 3. 检查键是否存在
	fmt.Println("\n3. 检查键是否存在:")
	redisClient.Exists("greeting")
	redisClient.Exists("nonexistent_key")

	// 4. 递增操作
	fmt.Println("\n4. 递增操作:")
	redisClient.Set("counter", "0", 0)
	redisClient.Increment("counter")
	redisClient.Increment("counter")
	redisClient.Get("counter")

	// 5. 删除操作
	fmt.Println("\n5. 删除操作:")
	redisClient.Set("to_delete", "将被删除的数据", 0)
	redisClient.Delete("to_delete")
	redisClient.Exists("to_delete")

	// 6. 列表操作
	fmt.Println("\n6. 列表操作:")
	redisClient.ListRPush("listKey2", "item1", "item2", "item3")
	redisClient.ListLPop("listKey2")
	length, err := redisClient.ListLLen("listKey2")
	if err != nil {
		log.Fatalf("获取列表长度失败: %v", err)
	}
	log.Printf("列表长度: %d", length)
	items, err := redisClient.ListLRange("listKey2", 0, length-1)
	if err != nil {
		log.Fatalf("获取列表元素失败: %v", err)
	}
	log.Printf("列表元素: %v", items)

	// 7. 哈希操作
	fmt.Println("\n7. 哈希操作:")
	redisClient.HashSet("user:1002", "name", "Alice", "age", "25", "email", "alice@example.com")
	redisClient.HashSet("user:1002", "name", "Alice", "age", "28", "email", "alice@example.com")
	redisClient.HashGetAll("user:1002")
	redisClient.HashGet("user:1002", "age")
	data := map[string]string{
		"name":  "Bob",
		"age":   "20",
		"email": "bob@example.com",
	}
	redisClient.HashSet("user:1003", data)
	redisClient.HashGetAll("user:1003")
	redisClient.HashGet("user:1003", "email")

	// 8. Set集合操作
	fmt.Println("\n8. Set集合操作:")
	redisClient.SetSAdd("myset2", "item1", "item2", "item3")
	redisClient.SetSMembers("myset2")
	redisClient.SetSIsMember("myset2", "item3")
	redisClient.SetSCard("myset2")
	redisClient.SetSRandMember("myset2")
	redisClient.SetSRem("myset2", "item3", "item1")
	redisClient.SetSMembers("myset2")

	// 9. 有序集合操作
	fmt.Println("\n9. 有序集合操作:")
	members := []redis.Z{
		{Score: 60, Member: "Tim"},
		{Score: 75, Member: "Green"},
		{Score: 80, Member: "Jone"},
		{Score: 30, Member: "Lucy"},
	}
	redisClient.SetZAdd("myzset2", members...)
	redisClient.SetZAdd("myzset2", redis.Z{Score: 45, Member: "Lucy"})
	redisClient.SetZCard("myzset2")
	redisClient.SetZRange("myzset2", 0, -1)
	redisClient.SetZRange("myzset2", 0, 1)
	redisClient.SetZRevRange("myzset2", 0, -1)
	redisClient.SetZRangeByScore("myzset2", "60", "75", 0, -1)
	redisClient.SetZScore("myzset2", "Jone")
	redisClient.SetZIncrBy("myzset2", "Jone", 10)
	redisClient.SetZRank("myzset2", "Jone")
	redisClient.SetZRevRank("myzset2", "Jone")
	redisClient.SetZRem("myzset2", "Lucy")
	redisClient.SetZRange("myzset2", 0, -1)

	fmt.Println("\n=== 演示完成 ===")
}
