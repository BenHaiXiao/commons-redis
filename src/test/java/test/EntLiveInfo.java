/*
 * Copyright (c) 2015 yy.com. 
 *
 * All Rights Reserved.
 *
 * This program is the confidential and proprietary information of 
 * YY.INC. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with yy.com.
 */
package test;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * @author xiaobenhai
 */
public class EntLiveInfo implements Serializable {
	private static final long serialVersionUID = -2312326881908163461L;
	/** 频道SID */
	private long sid;
	/** 子频道SID */
	private long ssid;
	/** 直播图片 */
	private String thumb = "";
	/** 直播uid */
	private long liveId;
	/** 直播间名称 */
	private String liveName = "";
	/** 直播间名称显示样式 */
	private int nameStyle;
	/** 已经开播时间 (分钟) */
	private long liveTime;
	/** 开始时刻 (毫秒数字) */
	private long start;
	/** 在线用户数 */
	private int users;
	/**
	 * 直播类型 :1-视频 2-音频
	 * */
	private int liveType;
	/**
	 * 是否显示推荐图标 :1-显示 2-不显示
	 * */
	private int specificRecommend;
	/** 签约频道 */
	private Long contractCid;
	
	//v4.x增加的属性
	/**标签模板*/
	private Integer tagId = 0;
	/**标签名称*/
	private String tagContent;
	/**直播描述*/
	private String liveDesc;
	/**艺名*/
	private String stageName;
	
	/** 头像 */
	private String avatar;

	public EntLiveInfo() {
	}

	public long getSid() {
		return sid;
	}

	public void setSid(long sid) {
		this.sid = sid;
	}

	public long getSsid() {
		return ssid;
	}

	public void setSsid(long ssid) {
		this.ssid = ssid;
	}

	public String getThumb() {
		return thumb;
	}

	public void setThumb(String thumb) {
		this.thumb = StringUtils.trimToEmpty(thumb);
	}

	public long getLiveId() {
		return liveId;
	}

	public void setLiveId(long liveId) {
		this.liveId = liveId;
	}

	public String getLiveName() {
		return liveName;
	}

	public void setLiveName(String liveName) {
		this.liveName = StringUtils.trimToEmpty(liveName);
	}

	public long getLiveTime() {
		return liveTime;
	}

	public void setLiveTime(long liveTime) {
		this.liveTime = liveTime;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public int getUsers() {
		return users;
	}

	public void setUsers(int users) {
		this.users = users;
	}

	public int getLiveType() {
		return liveType;
	}

	public void setLiveType(int liveType) {
		this.liveType = liveType;
	}

	public int getSpecificRecommend() {
		return specificRecommend;
	}

	public void setSpecificRecommend(int specificRecommend) {
		this.specificRecommend = specificRecommend;
	}
	
	public int getNameStyle() {
		return nameStyle;
	}

	public void setNameStyle(int nameStyle) {
		this.nameStyle = nameStyle;
	}

	public Long getContractCid() {
		return contractCid;
	}

	public void setContractCid(Long contractCid) {
		this.contractCid = contractCid;
	}

	public Integer getTagId() {
		return tagId;
	}

	public void setTagId(Integer tagId) {
		this.tagId = tagId;
	}

	public String getTagContent() {
		return tagContent;
	}

	public void setTagContent(String tagContent) {
		this.tagContent = tagContent;
	}

	public String getLiveDesc() {
		return liveDesc;
	}

	public void setLiveDesc(String liveDesc) {
		this.liveDesc = liveDesc;
	}

	public String getStageName() {
		return stageName;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}
	
	public String getAvatar() {
		return avatar;
	}

	public void setAvatar(String avatar) {
		this.avatar = avatar;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
