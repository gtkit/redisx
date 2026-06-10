package redisx

import (
	"maps"
	"slices"
	"strings"
)

// InitError 聚合降级初始化（[WithAllowPartialInit]）下各 DB 的失败详情。
//
// 通过 [errors.As] 提取后可按 DB 编程决策：
//
//	var ie *redisx.InitError
//	if errors.As(err, &ie) {
//	    if _, bad := ie.Failed[2]; bad {
//	        // DB2 不可用，按业务决定降级或拒绝启动
//	    }
//	}
type InitError struct {
	// Failed 记录每个初始化失败的 DB 编号及其失败原因。
	Failed map[int]error
}

// Error 按 DB 编号升序稳定输出所有失败原因。
func (e *InitError) Error() string {
	dbs := slices.Sorted(maps.Keys(e.Failed))
	parts := make([]string, 0, len(dbs))
	for _, db := range dbs {
		parts = append(parts, e.Failed[db].Error())
	}
	return "redisx: partial init failed: " + strings.Join(parts, "; ")
}

// Unwrap 按 DB 编号升序返回各失败的底层错误，
// 使 [errors.Is] / [errors.As] 可穿透到网络层错误。
func (e *InitError) Unwrap() []error {
	dbs := slices.Sorted(maps.Keys(e.Failed))
	errs := make([]error, 0, len(dbs))
	for _, db := range dbs {
		errs = append(errs, e.Failed[db])
	}
	return errs
}
