// sql92_token_test.go：词法器行为契约测试。
//
// 职责：
//   - 锁定 lex 的 token 切分、关键字规范化、字符串转义与错误形态
//
// 边界：
//   - 只测词法层，不测语法/求值（那是 sql92_parse_test.go / sql92_eval 的事）
package deliver

import (
	"strings"
	"testing"
)

func TestLexBasic(t *testing.T) {
	toks, err := lex("age >= 18 AND name = 'bob'")
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	want := []token{
		{kind: tokIdent, text: "age", col: 1},
		{kind: tokOp, text: ">=", col: 5},
		{kind: tokNumber, text: "18", col: 8},
		{kind: tokKeyword, text: "AND", col: 11},
		{kind: tokIdent, text: "name", col: 15},
		{kind: tokOp, text: "=", col: 20},
		{kind: tokString, text: "bob", col: 22},
		// 表达式共 26 字节，EOF 列号 = 26 + 1 = 27。
		{kind: tokEOF, text: "", col: 27},
	}
	if len(toks) != len(want) {
		t.Fatalf("token 数 = %d，期望 %d：%+v", len(toks), len(want), toks)
	}
	for i := range want {
		if toks[i] != want[i] {
			t.Fatalf("token[%d] = %+v，期望 %+v", i, toks[i], want[i])
		}
	}
}

func TestLexKeywordCaseInsensitive(t *testing.T) {
	// 关键字大小写不敏感且统一规范化为大写，解析器因此只需比对大写形式。
	for _, expr := range []string{"a is not null", "a IS NOT NULL", "a Is Not Null"} {
		toks, err := lex(expr)
		if err != nil {
			t.Fatalf("lex(%q): %v", expr, err)
		}
		got := []string{toks[1].text, toks[2].text, toks[3].text}
		if got[0] != "IS" || got[1] != "NOT" || got[2] != "NULL" {
			t.Fatalf("lex(%q) 关键字未规范化为大写: %v", expr, got)
		}
	}
}

func TestLexIdentCaseSensitive(t *testing.T) {
	// 属性名必须原样保留：age 与 Age 是两个不同的属性。
	toks, _ := lex("Age = 1")
	if toks[0].text != "Age" {
		t.Fatalf("属性名被改写为 %q，期望原样 %q", toks[0].text, "Age")
	}
}

func TestLexStringEscape(t *testing.T) {
	// '' 表示一个单引号字符。
	toks, err := lex("k = 'it''s'")
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	if toks[2].kind != tokString || toks[2].text != "it's" {
		t.Fatalf("字符串常量 = %+v，期望 tokString \"it's\"", toks[2])
	}
}

func TestLexStringKeepsKeywords(t *testing.T) {
	// 字符串字面量里的 AND 不得被当成关键字切开——这是把 SQL92 交给
	// 通用表达式库做字符串预处理时最典型的翻车点，手写词法器必须挡住。
	toks, err := lex("k = 'A AND B'")
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	if toks[2].kind != tokString || toks[2].text != "A AND B" {
		t.Fatalf("字符串被切开: %+v", toks[2])
	}
	if toks[3].kind != tokEOF {
		t.Fatalf("字符串之后应到 EOF，实际 %+v", toks[3])
	}
}

func TestLexNegativeNumber(t *testing.T) {
	toks, err := lex("t > -3.5")
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	if toks[2].kind != tokNumber || toks[2].text != "-3.5" {
		t.Fatalf("负数常量 = %+v，期望 tokNumber \"-3.5\"", toks[2])
	}
}

func TestLexErrors(t *testing.T) {
	cases := []struct {
		expr   string
		wantIn string // 错误串必须包含的片段
	}{
		{"k = 'unterminated", "未闭合"},
		{"k != 1", "!="},       // != 不被接受，报未知 token
		{"k = 1 @ 2", "@"},     // 未知字符原样出现在错误里
		{"k = 1.2.3", "1.2.3"}, // 非法数值
	}
	for _, c := range cases {
		_, err := lex(c.expr)
		if err == nil {
			t.Fatalf("lex(%q) 期望报错，实际 nil", c.expr)
		}
		if !strings.Contains(err.Error(), c.wantIn) {
			t.Fatalf("lex(%q) 错误 %q 未包含 %q", c.expr, err.Error(), c.wantIn)
		}
	}
}
