// sql92_token.go：SQL92 过滤表达式的词法分析。
//
// 职责：
//   - 把表达式字符串切成 token 流，每个 token 带 1-based 字节列号
//   - 关键字识别与大写规范化；属性名原样保留（大小写敏感）
//   - 字符串常量的 ” 转义
//
// 边界：
//   - 不做任何语法结构判断（那是 sql92_parse.go 的事）：本文件只回答
//     "下一个 token 是什么"，不回答"它出现在这里合不合法"
//   - 不接受 !=、科学计数法、十六进制：本 spec 的基准是 RocketMQ 文档化
//     子集，不擅自扩展同义写法（见 spec §3.1/§3.2）
//   - 表达式语法限定 ASCII：非 ASCII 字节只可能出现在字符串常量内，
//     按字节透传即可，无需处理多字节序列
package deliver

import (
	"fmt"
	"strconv"
	"strings"
)

// tokenKind token 的种类。
type tokenKind uint8

const (
	// tokEOF 输入结束。lex 保证最后一个 token 一定是它。
	tokEOF tokenKind = iota
	// tokIdent 属性名，原文保留，大小写敏感。
	tokIdent
	// tokNumber 数值常量（含前导负号），text 为原文。
	tokNumber
	// tokString 字符串常量，text 为已去掉引号并完成 '' 转义的内容。
	tokString
	// tokKeyword SQL92 关键字，text 恒为大写形式。
	tokKeyword
	// tokOp 比较运算符：= <> > >= < <=，text 为运算符原文。
	tokOp
	// tokLParen 左括号。
	tokLParen
	// tokRParen 右括号。
	tokRParen
	// tokComma 逗号。
	tokComma
)

// token 一个词法单元。
type token struct {
	kind tokenKind
	text string
	col  int // 1-based 字节列号，指向 token 起始字节
}

// keywords SQL92 关键字表。比对恒用大写形式。
var keywords = map[string]bool{
	"AND":     true,
	"OR":      true,
	"NOT":     true,
	"BETWEEN": true,
	"IN":      true,
	"IS":      true,
	"NULL":    true,
	"TRUE":    true,
	"FALSE":   true,
}

// lex 把 SQL92 过滤表达式切成 token 流。
//
// 参数：
//   - expr: 表达式原文，逐字节扫描，不做 Unicode 处理
//
// 返回：
//   - 成功时 token 流末尾恒有 tokEOF，其 col 为 len(expr)+1
//   - 失败时返回 error，错误串形如「第 N 列：原因 + 具体内容」，
//     调用方可原样回给客户端
func lex(expr string) ([]token, error) {
	toks := make([]token, 0, 16)
	i, n := 0, len(expr)
	for i < n {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++

		case c == '(':
			toks = append(toks, token{kind: tokLParen, text: "(", col: i + 1})
			i++
		case c == ')':
			toks = append(toks, token{kind: tokRParen, text: ")", col: i + 1})
			i++
		case c == ',':
			toks = append(toks, token{kind: tokComma, text: ",", col: i + 1})
			i++

		case c == '=':
			toks = append(toks, token{kind: tokOp, text: "=", col: i + 1})
			i++

		case c == '<' || c == '>':
			// 长扫描：`<` 与 `>` 可能带 `=` 后缀，`<>` 是合法的「不等于」。
			start := i
			if i+1 < n && expr[i+1] == '=' {
				i += 2
			} else if c == '<' && i+1 < n && expr[i+1] == '>' {
				i += 2
			} else {
				i++
			}
			toks = append(toks, token{kind: tokOp, text: expr[start:i], col: start + 1})

		case c == '-':
			// 负号并入紧随的数值 token。为什么敢这么合并：文法里 `-` 不作
			// 为二元运算符出现（不支持算术），所以它后面跟数字时只可能是
			// 负数常量，不存在 `a - 1` 这种会把负号切错的歧义。
			start := i
			i++
			if i < n && expr[i] >= '0' && expr[i] <= '9' {
				for i < n && (expr[i] >= '0' && expr[i] <= '9' || expr[i] == '.') {
					i++
				}
				text := expr[start:i]
				if _, err := strconv.ParseFloat(text, 64); err != nil {
					return nil, fmt.Errorf("第 %d 列：非法数值常量 %q", start+1, text)
				}
				toks = append(toks, token{kind: tokNumber, text: text, col: start + 1})
			} else {
				return nil, fmt.Errorf("第 %d 列：无法识别的字符 %q", start+1, "-")
			}

		case c >= '0' && c <= '9':
			start := i
			for i < n && (expr[i] >= '0' && expr[i] <= '9' || expr[i] == '.') {
				i++
			}
			text := expr[start:i]
			if _, err := strconv.ParseFloat(text, 64); err != nil {
				return nil, fmt.Errorf("第 %d 列：非法数值常量 %q", start+1, text)
			}
			toks = append(toks, token{kind: tokNumber, text: text, col: start + 1})

		case c == '\'':
			start := i
			i++
			var sb strings.Builder
			closed := false
			for i < n {
				if expr[i] == '\'' {
					if i+1 < n && expr[i+1] == '\'' {
						// '' 表示一个单引号字符，跳过两个引号并写入一个。
						sb.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				sb.WriteByte(expr[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("第 %d 列：字符串常量未闭合", start+1)
			}
			toks = append(toks, token{kind: tokString, text: sb.String(), col: start + 1})

		case isIdentStart(c):
			start := i
			for i < n && isIdentPart(expr[i]) {
				i++
			}
			text := expr[start:i]
			if keywords[strings.ToUpper(text)] {
				toks = append(toks, token{kind: tokKeyword, text: strings.ToUpper(text), col: start + 1})
			} else {
				toks = append(toks, token{kind: tokIdent, text: text, col: start + 1})
			}

		case c == '!':
			// `!` 单独不成 token，但 `!=` 是最可能被误写的形式，给显式提示。
			if i+1 < n && expr[i+1] == '=' {
				return nil, fmt.Errorf("第 %d 列：无法识别的 token %q", i+1, "!=")
			}
			return nil, fmt.Errorf("第 %d 列：无法识别的字符 %q", i+1, expr[i])

		default:
			return nil, fmt.Errorf("第 %d 列：无法识别的字符 %q", i+1, expr[i])
		}
	}
	toks = append(toks, token{kind: tokEOF, text: "", col: n + 1})
	return toks, nil
}

// isIdentStart 标识符（属性名）首字符：字母或下划线。
func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// isIdentPart 标识符后续字符：首字符集合再加数字。
func isIdentPart(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9'
}
