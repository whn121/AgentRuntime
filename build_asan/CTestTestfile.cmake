# CMake generated Testfile for 
# Source directory: /home/whn/AgentRuntime
# Build directory: /home/whn/AgentRuntime/build_asan
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(runtime_engine "/home/whn/AgentRuntime/build_asan/test_runtime_engine")
set_tests_properties(runtime_engine PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;93;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(scheduler "/home/whn/AgentRuntime/build_asan/test_scheduler")
set_tests_properties(scheduler PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;103;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(task_model "/home/whn/AgentRuntime/build_asan/test_task_model")
set_tests_properties(task_model PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;108;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(worker "/home/whn/AgentRuntime/build_asan/test_worker")
set_tests_properties(worker PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;113;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(close_drain "/home/whn/AgentRuntime/build_asan/test_close_drain")
set_tests_properties(close_drain PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;118;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(scheduler_fairness "/home/whn/AgentRuntime/build_asan/test_scheduler_fairness")
set_tests_properties(scheduler_fairness PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;123;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(runtime_cancel "/home/whn/AgentRuntime/build_asan/test_runtime_cancel")
set_tests_properties(runtime_cancel PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;128;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(cancel_by_client_e2e "/home/whn/AgentRuntime/build_asan/test_cancel_by_client_e2e")
set_tests_properties(cancel_by_client_e2e PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;133;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(protocol "/home/whn/AgentRuntime/build_asan/test_protocol")
set_tests_properties(protocol PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;139;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(stop_escalation "/home/whn/AgentRuntime/build_asan/test_stop_escalation")
set_tests_properties(stop_escalation PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;145;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(token_bucket "/home/whn/AgentRuntime/build_asan/test_token_bucket")
set_tests_properties(token_bucket PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;151;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(hmac "/home/whn/AgentRuntime/build_asan/test_hmac")
set_tests_properties(hmac PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;157;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
