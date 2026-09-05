 #include "user_dao.h"
  #include "Logger.h"
  #include <iostream>

  int main() {
    Logger::instance().init(LogLevel::DEBUG, "user_dao_test.log", true);

    DbConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 3306;
    cfg.user = "root";
    cfg.passwd = "";
    cfg.db = "myrpc_im";

    UserDao dao;
    if (!dao.init(cfg)) { std::cerr << "init failed\n"; return 1; }
    std::cout << "init ok\n";

    std::string err;
    std::cout << "registerUser(alice)   = "
              << dao.registerUser("alice", "secret123", &err) << " (" << err << ")\n";
    std::cout << "registerUser(alice2)  = "
              << dao.registerUser("alice", "secret123", &err) << " (" << err << ")\n";
    std::cout << "verifyLogin(alice,wrong)  = " << dao.verifyLogin("alice", "wrong") << "\n";
    std::cout << "verifyLogin(alice,secret) = " << dao.verifyLogin("alice", "secret123") <<
  "\n";

    dao.registerUser("bob", "pw456", &err);
    std::cout << "addFriend(alice,bob)    = " << dao.addFriend("alice", "bob", &err)
              << " (" << err << ")\n";
    std::cout << "addFriend(again)        = " << dao.addFriend("alice", "bob", &err)
              << " (" << err << ")\n";
    std::cout << "addFriend(alice,nobody) = " << dao.addFriend("alice", "nobody", &err)
              << " (" << err << ")\n";

    auto list = dao.getFriendList("alice");
    std::cout << "alice friends:";
    for (auto &f : list) std::cout << " " << f;
    std::cout << "\n";
    return 0;
  }