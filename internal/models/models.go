package models

import (
	"encoding/xml"
	"time"
)

type BaseRow struct {
	ID   int    `xml:"Id,attr"`
	Type string `xml:",name"`
}

type User struct {
	BaseRow
	Reputation     int       `xml:"Reputation,attr"`
	CreationDate   time.Time `xml:"CreationDate,attr"`
	DisplayName    string    `xml:"DisplayName,attr"`
	LastAccessDate time.Time `xml:"LastAccessDate,attr,omitempty"`
	WebsiteUrl     string    `xml:"WebsiteUrl,attr,omitempty"`
	Location       string    `xml:"Location,attr,omitempty"`
	AboutMe        string    `xml:"AboutMe,attr,omitempty"`
	Views          int       `xml:"Views,attr,omitempty"`
	UpVotes        int       `xml:"UpVotes,attr,omitempty"`
	DownVotes      int       `xml:"DownVotes,attr,omitempty"`
	AccountId      int       `xml:"AccountId,attr,omitempty"`
}

type Post struct {
	BaseRow
	PostTypeID         int       `xml:"PostTypeId,attr"`
	AcceptedAnswerID   int       `xml:"AcceptedAnswerId,attr,omitempty"`
	CreationDate       time.Time `xml:"CreationDate,attr"`
	Score              int       `xml:"Score,attr"`
	ViewCount          int       `xml:"ViewCount,attr,omitempty"`
	Body               string    `xml:"Body,attr,omitempty"`
	OwnerUserID        int       `xml:"OwnerUserId,attr,omitempty"`
	LastEditorUserID   int       `xml:"LastEditorUserId,attr,omitempty"`
	LastEditDate       time.Time `xml:"LastEditDate,attr,omitempty"`
	LastActivityDate   time.Time `xml:"LastActivityDate,attr,omitempty"`
	Title              string    `xml:"Title,attr,omitempty"`
	Tags               string    `xml:"Tags,attr,omitempty"`
	AnswerCount        int       `xml:"AnswerCount,attr,omitempty"`
	CommentCount       int       `xml:"CommentCount,attr,omitempty"`
	FavoriteCount      int       `xml:"FavoriteCount,attr,omitempty"`
	ClosedDate         time.Time `xml:"ClosedDate,attr,omitempty"`
	ParentID           int       `xml:"ParentId,attr,omitempty"`
	CommunityOwnedDate time.Time `xml:"CommunityOwnedDate,attr,omitempty"`
}

type Comment struct {
	BaseRow
	PostID       int       `xml:"PostId,attr"`
	Score        int       `xml:"Score,attr,omitempty"`
	Text         string    `xml:"Text,attr"`
	CreationDate time.Time `xml:"CreationDate,attr"`
	UserID       int       `xml:"UserId,attr,omitempty"`
}

type Badge struct {
	BaseRow
	UserID   int       `xml:"UserId,attr"`
	Name     string    `xml:"Name,attr"`
	Date     time.Time `xml:"Date,attr"`
	Class    int       `xml:"Class,attr,omitempty"`
	TagBased bool      `xml:"TagBased,attr,omitempty"`
}

type PostHistory struct {
	BaseRow
	PostID            int       `xml:"PostId,attr"`
	RevisionGUID      string    `xml:"RevisionGUID,attr,omitempty"`
	CreationDate      time.Time `xml:"CreationDate,attr"`
	UserID            int       `xml:"UserId,attr,omitempty"`
	PostHistoryTypeID int       `xml:"PostHistoryTypeId,attr"`
	Text              string    `xml:"Text,attr,omitempty"`
	Comment           string    `xml:"Comment,attr,omitempty"`
}

type PostLink struct {
	BaseRow
	CreationDate  time.Time `xml:"CreationDate,attr"`
	PostID        int       `xml:"PostId,attr"`
	RelatedPostID int       `xml:"RelatedPostId,attr"`
	LinkTypeID    int       `xml:"LinkTypeId,attr"`
}

type Tag struct {
	BaseRow
	TagName       string `xml:"TagName,attr"`
	Count         int    `xml:"Count,attr"`
	ExcerptPostID int    `xml:"ExcerptPostId,attr,omitempty"`
	WikiPostID    int    `xml:"WikiPostId,attr,omitempty"`
}

type Vote struct {
	BaseRow
	PostID       int       `xml:"PostId,attr"`
	VoteTypeID   int       `xml:"VoteTypeId,attr"`
	UserID       int       `xml:"UserId,attr,omitempty"`
	CreationDate time.Time `xml:"CreationDate,attr"`
	BountyAmount int       `xml:"BountyAmount,attr,omitempty"`
}

type Row struct {
	XMLName xml.Name
	Attrs   []xml.Attr `xml:",any,attr"`
}
