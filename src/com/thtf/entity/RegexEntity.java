package com.thtf.entity;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class RegexEntity {
	// 冷水机组
    String LSJZ_Run_Regex;
    String LSJZ_ZNDB_Regex;
    // 冷冻泵
    String LDB_Run_Regex;
    String LDB_ZNDB_Regex;
    // 冷却泵
    String LQB_Run_Regex;
    String LQB_ZNDB_Regex;
    // 冷却塔
    String LT_Run_Regex;
    String LT_ZNDB_Regex;
    // 送风机
    String KT_Run_Regex;
    String KT_ZNDB_Regex;
    String KT_Hz_Regex;
    // 排风机
    String HPF_Run_Regex;
    String HPF_ZNDB_Regex;
    String HPF_Hz_Regex;
    // 温度
    String HF_T_Regex;
    String SF_T_Regex;
    String XF_T_Regex;
    String ZTX_T_Regex;
    String ZTS_T_Regex;
    // 冷冻水
    String LD_yield_Regex;
    String LD_IT_Regex;
    String LD_OT_Regex;
    // 冷却水
    String LQ_yield_Regex;
    String LQ_IT_Regex;
    String LQ_OT_Regex;
    // 总电耗
    String ALL_ZNDB_Regex;
	public RegexEntity() {
		// 读取配置文件
	    Properties defaults = new Properties();
	    try {
	      //params.load(new FileInputStream(new File("conf/params.properties")))
	    	defaults.load(new FileInputStream(new File("defaults.properties")));
	    } catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error reading configuration file!");
		}
		LSJZ_Run_Regex = defaults.getProperty("LSJZ_Run_Regex");
		LSJZ_ZNDB_Regex = defaults.getProperty("LSJZ_ZNDB_Regex");
		LDB_Run_Regex = defaults.getProperty("LDB_Run_Regex");
		LDB_ZNDB_Regex = defaults.getProperty("LDB_ZNDB_Regex");
		LQB_Run_Regex = defaults.getProperty("LQB_Run_Regex");
		LQB_ZNDB_Regex = defaults.getProperty("LQB_ZNDB_Regex");
		LT_Run_Regex = defaults.getProperty("LT_Run_Regex");
		LT_ZNDB_Regex = defaults.getProperty("LT_ZNDB_Regex");
		KT_Run_Regex = defaults.getProperty("KT_Run_Regex");
		KT_ZNDB_Regex = defaults.getProperty("KT_ZNDB_Regex");
		KT_Hz_Regex = defaults.getProperty("KT_Hz_Regex");
		HPF_Run_Regex = defaults.getProperty("HPF_Run_Regex");
		HPF_ZNDB_Regex = defaults.getProperty("HPF_ZNDB_Regex");
		HPF_Hz_Regex = defaults.getProperty("HPF_Hz_Regex");
		HF_T_Regex = defaults.getProperty("HF_T_Regex");
		SF_T_Regex = defaults.getProperty("SF_T_Regex");
		XF_T_Regex = defaults.getProperty("XF_T_Regex");
		ZTX_T_Regex = defaults.getProperty("ZTX_T_Regex");
		ZTS_T_Regex = defaults.getProperty("ZTS_T_Regex");
		LD_yield_Regex = defaults.getProperty("LD_yield_Regex");
		LD_IT_Regex = defaults.getProperty("LD_IT_Regex");
		LD_OT_Regex = defaults.getProperty("LD_OT_Regex");
		LQ_yield_Regex = defaults.getProperty("LQ_yield_Regex");
		LQ_IT_Regex = defaults.getProperty("LQ_IT_Regex");
		LQ_OT_Regex = defaults.getProperty("LQ_OT_Regex");
		ALL_ZNDB_Regex = defaults.getProperty("ALL_ZNDB_Regex");
	}
	public String getLSJZ_Run_Regex() {
		return LSJZ_Run_Regex;
	}
	public String getLSJZ_ZNDB_Regex() {
		return LSJZ_ZNDB_Regex;
	}
	public String getLDB_Run_Regex() {
		return LDB_Run_Regex;
	}
	public String getLDB_ZNDB_Regex() {
		return LDB_ZNDB_Regex;
	}
	public String getLQB_Run_Regex() {
		return LQB_Run_Regex;
	}
	public String getLQB_ZNDB_Regex() {
		return LQB_ZNDB_Regex;
	}
	public String getLT_Run_Regex() {
		return LT_Run_Regex;
	}
	public String getLT_ZNDB_Regex() {
		return LT_ZNDB_Regex;
	}
	public String getKT_Run_Regex() {
		return KT_Run_Regex;
	}
	public String getKT_ZNDB_Regex() {
		return KT_ZNDB_Regex;
	}
	public String getKT_Hz_Regex() {
		return KT_Hz_Regex;
	}
	public String getHPF_Run_Regex() {
		return HPF_Run_Regex;
	}
	public String getHPF_ZNDB_Regex() {
		return HPF_ZNDB_Regex;
	}
	public String getHPF_Hz_Regex() {
		return HPF_Hz_Regex;
	}
	public String getHF_T_Regex() {
		return HF_T_Regex;
	}
	public String getSF_T_Regex() {
		return SF_T_Regex;
	}
	public String getXF_T_Regex() {
		return XF_T_Regex;
	}
	public String getZTX_T_Regex() {
		return ZTX_T_Regex;
	}
	public String getZTS_T_Regex() {
		return ZTS_T_Regex;
	}
	public String getLD_yield_Regex() {
		return LD_yield_Regex;
	}
	public String getLD_IT_Regex() {
		return LD_IT_Regex;
	}
	public String getLD_OT_Regex() {
		return LD_OT_Regex;
	}
	public String getLQ_yield_Regex() {
		return LQ_yield_Regex;
	}
	public String getLQ_IT_Regex() {
		return LQ_IT_Regex;
	}
	public String getLQ_OT_Regex() {
		return LQ_OT_Regex;
	}
	public String getALL_ZNDB_Regex() {
		return ALL_ZNDB_Regex;
	}
}
