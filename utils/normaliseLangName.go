package utils

import "strings"

func NormalizeLanguage(lang string) string {

	lang = strings.ToLower(lang)

	languageMap := map[string]string{

		"js":          "js",
		"jscript":     "js",
		"javscript":   "js",
		"javsscript":  "js",
		"javascipt":   "js",
		"javasript":   "js",
		"javascript":  "js",
		"java script": "js",
		"jscipt":      "js",

		"python":  "python",
		"pyt":     "python",
		"pyn":     "python",
		"pythn":   "python",
		"phyton":  "python",
		"py":      "python",
		"py thon": "python",
		"pthon":   "python",

		"go":      "go",
		"golang":  "go",
		"gol":     "go",
		"goo":     "go",
		"g o":     "go",
		"golangg": "go",

		"cpp":    "cpp",
		"c++":    "cpp",
		"cp":     "cpp",
		"cppp":   "cpp",
		"c plus": "cpp",
		"cxx":    "cpp",
		"cc":     "cpp",
		"cpp ":   "cpp",
	}

	if normalized, ok := languageMap[lang]; ok {
		return normalized
	}

	return lang
}
