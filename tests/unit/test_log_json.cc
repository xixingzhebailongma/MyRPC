// JSON 日志转义边界单测：message 含 "、\、\n、\t、\0、超长字符串。
// 断言 jsonEscape 正确 + assembleJsonLine 输出可被 python3(json) 解析且字段值正确。
// 本机无 jq，用 python3 的 json 模块作等价替代。
#include "Logger.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <unistd.h>

static int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

// 用 python3 验证一段 JSON 是否可解析。
static bool pythonJsonParses(const std::string &json) {
  const char *path = "/tmp/test_log_json_tmp.json";
  FILE *f = std::fopen(path, "w");
  if (!f)
    return false;
  std::fwrite(json.data(), 1, json.size(), f);
  std::fclose(f);
  char cmd[512];
  std::snprintf(cmd, sizeof(cmd),
                "python3 -c \"import json;json.load(open('%s'))\" "
                ">/dev/null 2>&1",
                path);
  return std::system(cmd) == 0;
}

int main() {
  // 0) service 未设置 → init 应打 WARN 到 stderr（"service name not set"）。
  //    注意 init 里是 fprintf(stderr, …)（C stderr），不是 std::cerr，得重定向 fd 2。
  {
    char tmpl[] = "/tmp/test_log_json_stderr_XXXXXX";
    int capfd = ::mkstemp(tmpl);
    if (capfd < 0) return fail("mkstemp failed");
    int saved = ::dup(STDERR_FILENO);
    ::dup2(capfd, STDERR_FILENO);            // stderr → 临时文件
    Logger::instance().init(LogLevel::INFO, "", /*console=*/false);
    ::fflush(stderr);
    ::dup2(saved, STDERR_FILENO);            // 还原
    ::close(saved);

    std::ifstream in(tmpl);
    std::string cap((std::istreambuf_iterator<char>(in)),
                    std::istreambuf_iterator<char>());
    in.close();
    ::close(capfd);
    ::unlink(tmpl);
    if (cap.find("service name not set") == std::string::npos)
      return fail("service-not-set WARN not triggered");
  }

  // 1) jsonEscape 各转义字符
  if (Logger::jsonEscape("a\"b") != "a\\\"b")
    return fail("quote escape");
  if (Logger::jsonEscape("a\\b") != "a\\\\b")
    return fail("backslash escape");
  if (Logger::jsonEscape("a\nb") != "a\\nb")
    return fail("newline escape");
  if (Logger::jsonEscape("a\tb") != "a\\tb")
    return fail("tab escape");
  if (Logger::jsonEscape(std::string("a\0b", 3)) != "a\\u0000b")
    return fail("nul escape");

  // 2) 超长字符串（无特殊字符应原样，不越界、不破坏）
  std::string long_s(10000, 'x');
  if (Logger::jsonEscape(long_s) != long_s)
    return fail("long string escape");

  // 3) assembleJsonLine：message 含 " \ \n \t \0，python3 可解析 + 字段值正确
  Logger::instance().setServiceName("test");
  LogContext ctx;
  ctx.trace_id = "t1";
  ctx.request_id = "r1";
  // 内嵌 \0 的消息：用长度构造，确保 jsonEscape 转成 \u0000
  std::string msg = std::string("nul\0end", 7); // n,u,l,\0,e,n,d
  std::string line = Logger::instance().assembleJsonLine(
      LogLevel::INFO, "/x/y/test.cc", 42, ctx, msg);

  if (!pythonJsonParses(line))
    return fail("assembleJsonLine output not JSON-parseable by python3");

  if (line.find("\"service\":\"test\"") == std::string::npos)
    return fail("service field wrong");
  if (line.find("\"file\":\"test.cc\"") == std::string::npos)
    return fail("file basename wrong");
  if (line.find("\"line\":42") == std::string::npos)
    return fail("line field wrong");
  if (line.find("\"trace_id\":\"t1\"") == std::string::npos)
    return fail("trace_id field wrong");
  if (line.find("\"request_id\":\"r1\"") == std::string::npos)
    return fail("request_id field wrong");
  if (line.find("\"msg_id\":\"-\"") == std::string::npos)
    return fail("empty msg_id should be '-'");
  if (line.find("\"message\":\"nul\\u0000end\"") == std::string::npos)
    return fail("message nul escaping wrong");

  // 4) >4096 截断顺序：先 vsnprintf 截断 + "..."，再 jsonEscape，JSON 仍有效。
  //    走完整 vlog 路径（console 捕获 stdout）。
  {
    Logger::instance().init(LogLevel::INFO, "", /*console=*/true);
    std::string long_msg(5000, 'a');
    long_msg[4094] = '"'; // 截断边界（第 4095 字节）放一个引号
    std::ostringstream cap;
    std::streambuf *old = std::cout.rdbuf(cap.rdbuf());
    Logger::instance().log(LogLevel::INFO, "/x/y/f.cc", 1, "%s",
                           long_msg.c_str());
    std::cout.rdbuf(old);
    std::string truncated_line = cap.str();

    if (!pythonJsonParses(truncated_line))
      return fail(">4096 truncated line not JSON-parseable");
    if (truncated_line.find("...") == std::string::npos)
      return fail("truncation marker '...' missing");
    if (truncated_line.find("\\\"...") == std::string::npos)
      return fail("boundary quote not escaped before '...'");
  }

  std::printf("PASSED\n");
  return 0;
}
