#pragma once
#include <string>

class RpcController {
public:
  bool Failed() const { return failed_; }
  std::string ErrorText() const { return error_; }
  void Reset() {
      failed_ = false;
      error_ = "";
  }
  void SetFailed(const std::string &reason) { 
    failed_ = true;
    error_ = reason;
 }

private:
  bool failed_ = false;
  std::string error_;
};