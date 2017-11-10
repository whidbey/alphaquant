#include "broker_adapter.h"
#include "md5.h"
#include "lib.h"
#include <sstream>

using namespace std;

broker_adapter::broker_adapter(stock_broker *p)
	:
	m_broker_ptr(p),
	m_started(false)
{
	setState("idle", "");
}

broker_adapter::~broker_adapter()
{
	delete m_broker_ptr;
}

void broker_adapter::start()
{
	if (m_started) {
		return;
	}

	m_started = true;
	command cmd;
	cmd.cmd = START;

	ScopedLock lck(m_critical_sec);
	m_command_deq.push_back(cmd);
	m_event.Signal();

	StartThread();
}

void broker_adapter::stop()
{
	command cmd;
	cmd.cmd = STOP;

	ScopedLock lck(m_critical_sec);
	m_command_deq.push_back(cmd);
	m_event.Signal();
}

//{"sid": stockid, "action" : "open", "direction" : 1, "price" : price, "quant" : quant, "deal_quant" : 0, "deal_price" : 0, "state" : "pending", "desc" : "", "id" : user_order_id}

std::string broker_adapter::buy(const std::string& sid, int amount, double price, const std::string& order_type)
{
	std::string ret = gen_uuid();

	command cmd;
	cmd.cmd = BUY;
	cmd.sid = sid;
	cmd.quant = amount;
	cmd.price = price;
	cmd.order_type = order_type;
	cmd.order_id = ret;

	order_state ostate;
	ostate.action = "open";
	ostate.direction = 1;
	ostate.user_order_id = ret;
	ostate.state = "pending";
	ostate.price = price;
	ostate.sid = sid;
	ostate.quant = amount;
	ostate.price = price;

	ScopedLock lck(m_critical_sec);

	m_pending_order_map[ret] = ostate;

	m_command_deq.push_back(cmd);
	m_event.Signal();

	return ret;
}

std::string broker_adapter::sell(const std::string& sid, int amount, double price, const std::string& order_type)
{
	std::string ret = gen_uuid();

	command cmd;
	cmd.cmd = SELL;
	cmd.sid = sid;
	cmd.quant = amount;
	cmd.price = price;
	cmd.order_type = order_type;
	cmd.order_id = ret;

	order_state ostate;
	ostate.action = "close";
	ostate.direction = 1;
	ostate.user_order_id = ret;
	ostate.state = "pending";
	ostate.price = price;
	ostate.sid = sid;
	ostate.quant = amount;
	ostate.price = price;

	ScopedLock lck(m_critical_sec);
	
	m_pending_order_map[ret] = ostate;

	m_command_deq.push_back(cmd);

	m_event.Signal();
	return ret;
}

void broker_adapter::closeOrder(const std::string& order_id)
{
	command cmd;
	cmd.cmd = CANCEL;
	cmd.order_id = order_id;

	ScopedLock lck(m_critical_sec);
	m_command_deq.push_back(cmd);
	m_event.Signal();
}

std::vector<holding_item> broker_adapter::getHoldingStock()
{
	ScopedLock lck(m_critical_sec);

	return m_holding_stock;
}

order_state broker_adapter::getOrderState(const std::string& order_id)
{
	ScopedLock lck(m_critical_sec);

	if (m_pending_order_map.find(order_id) != m_pending_order_map.end()) {
		return m_pending_order_map[order_id];
	}

	if (m_completed_order_map.find(order_id) != m_completed_order_map.end()) {
		return m_completed_order_map[order_id];
	}

	return order_state();
}

std::string broker_adapter::getAccountState()
{
	ScopedLock lck(m_critical_sec);

	return m_state;
}

std::vector<double> broker_adapter::getBalance()
{
	ScopedLock lck(m_critical_sec);

	return m_balance;
}

int broker_adapter::getPendingOrderNum()
{
	ScopedLock lck(m_critical_sec);

	return m_pending_order_map.size();
}

u_int broker_adapter::Run()
{
	DWORD last_update_account = ::GetTickCount();
	try {
		while (true) {

			bool command_ok = false;
			int command_left = 0;
			command cmd;

			m_critical_sec.Lock();

			if (m_command_deq.size() > 0) {
				command_ok = true;
				cmd = m_command_deq[0];
				m_command_deq.pop_front();
			}

			m_critical_sec.Unlock();

			if (command_ok) {
				if (cmd.cmd == START) {
					int ret = m_broker_ptr->login();

					if (ret == 0) {
						setState("login", "login success");

						std::vector<holding_item> holding = m_broker_ptr->get_holding_stock();
						std::vector<double> balance = m_broker_ptr->get_money_left();

						ScopedLock lck(m_critical_sec);
						m_balance = balance;
						m_holding_stock = holding;
					}
					else {
						setState("failed", "login failed");
					}
				}
				else if (cmd.cmd == BUY) {
					std::string ret = m_broker_ptr->buy(cmd.sid, cmd.price, cmd.quant, cmd.order_type);

					ScopedLock lck(m_critical_sec);
					order_state &state = m_pending_order_map[cmd.order_id];

					if (ret.size() > 0)
						state.internal_order_id = ret;
					else {
						state.state = "failed";

						m_completed_order_map[cmd.order_id] = state;
						m_pending_order_map.erase(cmd.order_id);
					}
				}
				else if (cmd.cmd == SELL) {
					std::string ret = m_broker_ptr->sell(cmd.sid, cmd.price, cmd.quant, cmd.order_type);

					ScopedLock lck(m_critical_sec);
					order_state &state = m_pending_order_map[cmd.order_id];

					if (ret.size() > 0)
						state.internal_order_id = ret;
					else {
						state.state = "failed";

						m_completed_order_map[cmd.order_id] = state;
						m_pending_order_map.erase(cmd.order_id);
					}
				}
				else if (cmd.cmd == CANCEL) {
					std::string oi = cmd.order_id;
					std::string sid;
					std::string internal_id;

					m_critical_sec.Lock();
					if (m_pending_order_map.find(oi) != m_pending_order_map.end()) {
						sid = m_pending_order_map[oi].sid;
						internal_id = m_pending_order_map[oi].internal_order_id;
					}
					m_critical_sec.Unlock();

					if (sid.size() > 0 && internal_id.size() > 0) {
						m_broker_ptr->cancel_order(internal_id, sid);

						ScopedLock lck(m_critical_sec);
						order_state os = m_pending_order_map[oi];
						os.state = "canceled";
						m_completed_order_map[oi] = os;
						m_pending_order_map.erase(oi);
					}

				}
				else if (cmd.cmd == STOP) {

					break;
				}
			}
			else {
				DWORD current_tickcount = ::GetTickCount();
				if (current_tickcount - last_update_account > 400) {
					updateAccountState();
					last_update_account = current_tickcount;
				}
				m_event.Wait(100);
				handlePeriodic();
			}
		}

	}
	catch (std::exception& e) {
		setState("failed", e.what());
	}
	m_broker_ptr->logout();
	m_started = false;

	return 0;
}

void broker_adapter::updateAccountState()
{
	if (getAccountState() == "login") {

		std::vector<holding_item> holding = m_broker_ptr->get_holding_stock();
		std::vector<double> balance = m_broker_ptr->get_money_left();

		std::vector<internal_order_item> orders;
		if (getPendingOrderNum() > 0)
			orders = m_broker_ptr->get_all_order();

		ScopedLock lck(m_critical_sec);
		m_balance = balance;
		m_holding_stock = holding;

		for (int i = 0; i < orders.size(); ++i) {
			std::map<std::string, order_state>::iterator iter;
			for (iter = m_pending_order_map.begin(); iter != m_pending_order_map.end(); ++iter) {
				if (iter->second.internal_order_id == orders[i].order_id) {
					iter->second.deal_quant = orders[i].deal_quant;
					iter->second.deal_price = orders[i].deal_price;

					if (orders[i].deal_quant == orders[i].quant) {
						iter->second.state = "fulfilled";

						m_completed_order_map[iter->second.user_order_id] = iter->second;
						m_pending_order_map.erase(iter);
					}
					else {
						if (orders[i].deal_quant > 0) {
							iter->second.state = "partfilled";
						}
					}

					break;
				}
			}
		}
	}
}

void broker_adapter::setState(const std::string& state, const std::string& desc)
{
	ScopedLock lck(m_critical_sec);

	m_state = state;
	m_state_desc = desc;
}

void broker_adapter::handlePeriodic()
{
	std::string pkt;
	int ret = m_broker_ptr->try_recv_packet(pkt);
}