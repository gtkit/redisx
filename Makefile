.PHONY: tool check install-tools changelog tag release-patch release-minor gittag delcommit


LINT_TARGETS ?= ./...

# 发版语义级别：patch（默认）/ minor。major 被本项目策略拒绝（只做 v1）。
BUMP ?= patch
# 1=发版时用 git-chglog 生成 CHANGELOG.md 并纳入发版提交（会整篇重写，慎用）
AUTO_CHANGELOG ?= 0
tool: ## Lint Go code with the installed golangci-lint
	@ echo "▶️ golangci-lint run"
	golangci-lint run $(LINT_TARGETS)
	gofumpt -l -w .
	@ echo "✅ golangci-lint run"

## govulncheck 检查漏洞 go install golang.org/x/vuln/cmd/govulncheck@latest
check:
	govulncheck ./...
	gosec ./...

install-tools: ## 一键安装开发/发版所需工具
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install mvdan.cc/gofumpt@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install github.com/git-chglog/git-chglog/cmd/git-chglog@latest

## 用 git-chglog 生成/刷新 CHANGELOG.md（首次需先 git-chglog --init）。会整篇重写，生成后请人工 review。
changelog:
	@command -v git-chglog >/dev/null 2>&1 || { echo "✗ 未安装 git-chglog，先执行 make install-tools"; exit 1; }
	@[ -f .chglog/config.yml ] || { echo "✗ 未初始化 git-chglog，先执行一次 git-chglog --init"; exit 1; }
	git-chglog -o CHANGELOG.md
	@echo "✅ 已更新 CHANGELOG.md，请 review 后再提交/发版"
tag:
	@set -e; \
	if [ -n "$$(git status --porcelain)" ]; then \
		echo "✗ 工作区不干净，发版前请先提交或清理："; git status --short; exit 1; \
	fi; \
	echo "▶️ go vet"; go vet ./...; \
	echo "▶️ 测试 (race)"; go test -race -count=1 -timeout=5m ./...; \
	current=$$(grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' version.go | head -n1 | tr -d 'v'); \
	if [ -z "$$current" ]; then echo "version not found in version.go"; exit 1; fi; \
	maj=$$(echo $$current | cut -d. -f1); \
	min=$$(echo $$current | cut -d. -f2); \
	patch=$$(echo $$current | cut -d. -f3); \
	case "$(BUMP)" in \
	  patch) new="v$$maj.$$min.$$((patch+1))" ;; \
	  minor) new="v$$maj.$$((min+1)).0" ;; \
	  major) echo "✗ 本项目只做 v1、不发 v2；且 MAJOR 还需 /v2 module path 重构（仅 bump tag 是错误发布），已拒绝"; exit 1 ;; \
	  *) echo "✗ BUMP 必须为 patch 或 minor（当前: $(BUMP)）"; exit 1 ;; \
	esac; \
	printf "Bump (%s): v%s -> %s\n" "$(BUMP)" "$$current" "$$new"; \
	sed -E -i.bak 's/(const Version = ")([^"]+)(")/\1'"$$new"'\3/' version.go; \
	rm -f version.go.bak; \
	if [ "$(AUTO_CHANGELOG)" = "1" ]; then \
	  command -v git-chglog >/dev/null 2>&1 || { echo "✗ AUTO_CHANGELOG=1 但未装 git-chglog（make install-tools）"; exit 1; }; \
	  [ -f .chglog/config.yml ] || { echo "✗ AUTO_CHANGELOG=1 但未初始化 git-chglog（git-chglog --init）"; exit 1; }; \
	  echo "▶️ 生成 CHANGELOG.md（$$new）"; \
	  git-chglog --next-tag "$$new" -o CHANGELOG.md; \
	  git add CHANGELOG.md; \
	fi; \
	git add version.go; \
	git commit -m "chore(release): $$new"; \
	git tag -a "$$new" -m "release $$new"; \
	git push gtkit HEAD; \
	git push gtkit "$$new"; \
	printf "Done: %s\n" "$$new"

release-patch: ## 发布 PATCH 版本（bug 修复 / 文档 / 内部重构）
	@$(MAKE) tag BUMP=patch

release-minor: ## 发布 MINOR 版本（向后兼容新增导出 API / Option）
	@$(MAKE) tag BUMP=minor

gittag:
	git tag --sort=-version:refname | head -1

## 删除最近一次提交，但保留修改内容
delcommit:
	git reset --soft HEAD~1
