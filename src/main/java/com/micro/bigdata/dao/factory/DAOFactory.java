package com.micro.bigdata.dao.factory;


import com.micro.bigdata.dao.*;
import com.micro.bigdata.dao.impl.*;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}

	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}

	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionDAOImpl();
	}

	public static IAdBlacklistDAO getAdBlacklistDAO(){
		return new AdBlacklistDAOImpl();
	}

	public static IAdUserClickCountDAO getAdUserClickCountDAO(){
		return new AdUserClickCountDAOImpl();
	}

	public static IAdStatDAO getAdStatDAO(){
		return new AdStatDAOImpl();
	}

	public static IAdProvinceTop3DAO getAdProvinceTop3DAO(){
		return new AdProvinceTop3DAOImpl();
	}

	public static IAdClickTrendDAO getAdClickTrendDAO(){
		return new AdClickTrendDAOImpl();
	}

	public static IUserLoginLogDao getUserLoginLogDao(){
		return new UserLoginLogDaoImpl();
	}

	public static IOrgSwitchDAO getOrgSwitchDAO(){
		return new OrgSwitchDAOImpl();
	}

	public static ILoginDetailDAO getLoginDetailDAO(){
		return new LoginDetailDAOImpl();
	}

	public static ILoginCountDAO getLoginCountDAO(){
		return new LoginCountDAOImpl();
	}
}
