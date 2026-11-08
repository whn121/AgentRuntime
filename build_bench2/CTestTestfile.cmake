# CMake generated Testfile for 
# Source directory: /home/whn/AgentRuntime
# Build directory: /home/whn/AgentRuntime/build_bench2
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(runtime_engine "/home/whn/AgentRuntime/build_bench2/test_runtime_engine")
set_tests_properties(runtime_engine PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;83;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(scheduler "/home/whn/AgentRuntime/build_bench2/test_scheduler")
set_tests_properties(scheduler PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;93;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(task_model "/home/whn/AgentRuntime/build_bench2/test_task_model")
set_tests_properties(task_model PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;98;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(worker "/home/whn/AgentRuntime/build_bench2/test_worker")
set_tests_properties(worker PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;103;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(close_drain "/home/whn/AgentRuntime/build_bench2/test_close_drain")
set_tests_properties(close_drain PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;108;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(scheduler_fairness "/home/whn/AgentRuntime/build_bench2/test_scheduler_fairness")
set_tests_properties(scheduler_fairness PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;113;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(runtime_cancel "/home/whn/AgentRuntime/build_bench2/test_runtime_cancel")
set_tests_properties(runtime_cancel PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;118;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(cancel_by_client_e2e "/home/whn/AgentRuntime/build_bench2/test_cancel_by_client_e2e")
set_tests_properties(cancel_by_client_e2e PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;123;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(protocol "/home/whn/AgentRuntime/build_bench2/test_protocol")
set_tests_properties(protocol PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;129;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(stop_escalation "/home/whn/AgentRuntime/build_bench2/test_stop_escalation")
set_tests_properties(stop_escalation PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;135;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
add_test(token_bucket "/home/whn/AgentRuntime/build_bench2/test_token_bucket")
set_tests_properties(token_bucket PROPERTIES  _BACKTRACE_TRIPLES "/home/whn/AgentRuntime/CMakeLists.txt;141;add_test;/home/whn/AgentRuntime/CMakeLists.txt;0;")
