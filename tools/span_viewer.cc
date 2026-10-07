// span_viewer：读各服务导出的 spans_*.jsonl，按 trace_id 分组、按 parent_span_id
// 拼成调用树并打印每跳耗时。纯 C++ 实现，零依赖。
//
// 用法：span_viewer <目录|spans_*.jsonl> [更多...]
//   - 传目录：聚合该目录下所有 spans_*.jsonl。
//   - 传文件：直接读。
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <dirent.h>
#include <fstream>
#include <map>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace {

struct Span {
  std::string trace_id;
  std::string span_id;
  std::string parent_span_id;
  std::string service;
  std::string method;
  std::string status;
  uint64_t start_us = 0;
  uint64_t end_us = 0;
  uint64_t wall_us = 0;
};

// ---- 极简 JSON 解析：只面向 span 的扁平对象 schema ----
// 值只有两类：字符串（trace_id/span_id/.../status）与非负整数（*_us）。

void skipWs(const std::string &s, size_t &i) {
  while (i < s.size() &&
         (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r'))
    ++i;
}

int hexVal(char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  return -1;
}

// 解析 "..." 字符串，处理常见 JSON 转义。
bool parseString(const std::string &s, size_t &i, std::string &out) {
  if (i >= s.size() || s[i] != '"')
    return false;
  ++i;
  out.clear();
  while (i < s.size()) {
    char c = s[i];
    if (c == '"') {
      ++i;
      return true;
    }
    if (c == '\\') {
      ++i;
      if (i >= s.size())
        return false;
      char e = s[i++];
      switch (e) {
      case '"': out += '"'; break;
      case '\\': out += '\\'; break;
      case '/': out += '/'; break;
      case 'n': out += '\n'; break;
      case 't': out += '\t'; break;
      case 'r': out += '\r'; break;
      case 'b': out += '\b'; break;
      case 'f': out += '\f'; break;
      case 'u': {
        if (i + 4 > s.size())
          return false;
        int code = 0;
        for (int k = 0; k < 4; ++k) {
          int v = hexVal(s[i + k]);
          if (v < 0)
            return false;
          code = (code << 4) | v;
        }
        i += 4;
        out += static_cast<char>(code); // span 值都是 ASCII
        break;
      }
      default:
        return false;
      }
    } else {
      out += c;
      ++i;
    }
  }
  return false;
}

// 解析非负整数。
bool parseNumber(const std::string &s, size_t &i, uint64_t &out) {
  size_t j = i;
  uint64_t v = 0;
  bool any = false;
  while (j < s.size() && s[j] >= '0' && s[j] <= '9') {
    v = v * 10 + static_cast<uint64_t>(s[j] - '0');
    ++j;
    any = true;
  }
  if (!any)
    return false;
  i = j;
  out = v;
  return true;
}

// 跳过未知字段的值（只可能是字符串或数字/true/false/null）。
bool skipValue(const std::string &s, size_t &i) {
  skipWs(s, i);
  if (i >= s.size())
    return false;
  char c = s[i];
  if (c == '"') {
    std::string tmp;
    return parseString(s, i, tmp);
  }
  if (c == '-' || (c >= '0' && c <= '9')) {
    while (i < s.size() && (s[i] == '-' || s[i] == '+' || s[i] == '.' ||
                            s[i] == 'e' || s[i] == 'E' ||
                            (s[i] >= '0' && s[i] <= '9')))
      ++i;
    return true;
  }
  if (s.compare(i, 4, "true") == 0) { i += 4; return true; }
  if (s.compare(i, 5, "false") == 0) { i += 5; return true; }
  if (s.compare(i, 4, "null") == 0) { i += 4; return true; }
  return false;
}

// 解析一行 span JSON。
bool parseSpan(const std::string &line, Span &span) {
  size_t i = 0;
  skipWs(line, i);
  if (i >= line.size() || line[i] != '{')
    return false;
  ++i;
  while (true) {
    skipWs(line, i);
    if (i < line.size() && line[i] == '}')
      return true; // 空对象 / 正常结束
    std::string key;
    if (!parseString(line, i, key))
      return false;
    skipWs(line, i);
    if (i >= line.size() || line[i] != ':')
      return false;
    ++i;
    skipWs(line, i);

    if (key == "trace_id" || key == "span_id" || key == "parent_span_id" ||
        key == "service" || key == "method" || key == "status") {
      std::string v;
      if (!parseString(line, i, v))
        return false;
      if (key == "trace_id") span.trace_id = v;
      else if (key == "span_id") span.span_id = v;
      else if (key == "parent_span_id") span.parent_span_id = v;
      else if (key == "service") span.service = v;
      else if (key == "method") span.method = v;
      else span.status = v;
    } else if (key == "start_us" || key == "end_us" || key == "wall_us") {
      uint64_t v = 0;
      if (!parseNumber(line, i, v))
        return false;
      if (key == "start_us") span.start_us = v;
      else if (key == "end_us") span.end_us = v;
      else span.wall_us = v;
    } else {
      if (!skipValue(line, i))
        return false;
    }

    skipWs(line, i);
    if (i < line.size() && line[i] == ',') {
      ++i;
      continue;
    }
    if (i < line.size() && line[i] == '}')
      return true;
    return false;
  }
}

bool loadFile(const std::string &path, std::vector<Span> &out) {
  std::ifstream f(path);
  if (!f)
    return false;
  std::string line;
  while (std::getline(f, line)) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    Span s;
    if (parseSpan(line, s))
      out.push_back(std::move(s));
  }
  return true;
}

bool isDir(const std::string &path) {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// 列出目录里的 spans_*.jsonl（按文件名排序）。
void listSpanFiles(const std::string &dir, std::vector<std::string> &out) {
  DIR *d = opendir(dir.c_str());
  if (!d)
    return;
  struct dirent *e;
  while ((e = readdir(d)) != nullptr) {
    std::string name = e->d_name;
    if (name.size() >= 6 && name.compare(0, 6, "spans_") == 0 &&
        name.size() >= 6 && name.compare(name.size() - 6, 6, ".jsonl") == 0) {
      out.push_back(dir + "/" + name);
    }
  }
  closedir(d);
  std::sort(out.begin(), out.end());
}

using Children = std::map<std::string, std::vector<Span>>;

void printNode(const Span &node, const Children &children, int indent) {
  uint64_t dur = node.end_us >= node.start_us ? node.end_us - node.start_us : 0;
  std::string pad(static_cast<size_t>(indent) * 2, ' ');
  std::printf("%s%s.%s (%lluus, %s)\n", pad.c_str(), node.service.c_str(),
              node.method.c_str(), static_cast<unsigned long long>(dur),
              node.status.c_str());
  auto it = children.find(node.span_id);
  if (it == children.end())
    return;
  std::vector<Span> kids = it->second;
  std::sort(kids.begin(), kids.end(),
            [](const Span &a, const Span &b) { return a.wall_us < b.wall_us; });
  for (const auto &k : kids)
    printNode(k, children, indent + 1);
}

} // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: span_viewer <dir|spans_*.jsonl> [more ...]\n");
    return 1;
  }

  std::vector<std::string> files;
  for (int a = 1; a < argc; ++a) {
    std::string arg = argv[a];
    if (isDir(arg))
      listSpanFiles(arg, files);
    else
      files.push_back(arg);
  }

  std::vector<Span> spans;
  for (const auto &f : files)
    loadFile(f, spans);

  std::map<std::string, std::vector<Span>> byTrace;
  for (auto &s : spans)
    byTrace[s.trace_id].push_back(std::move(s));

  for (const auto &kv : byTrace) {
    const std::string &tid = kv.first;
    const std::vector<Span> &group = kv.second;

    std::map<std::string, Span> nodes;
    for (const auto &s : group)
      nodes[s.span_id] = s;
    Children children;
    std::vector<Span> roots;
    for (const auto &s : group) {
      if (!s.parent_span_id.empty() && nodes.count(s.parent_span_id))
        children[s.parent_span_id].push_back(s);
      else
        roots.push_back(s);
    }

    std::printf("=== trace %s (%zu spans) ===\n", tid.c_str(), group.size());
    std::sort(roots.begin(), roots.end(),
              [](const Span &a, const Span &b) { return a.wall_us < b.wall_us; });
    for (const auto &r : roots)
      printNode(r, children, 0);
    std::printf("\n");
  }
  return 0;
}
