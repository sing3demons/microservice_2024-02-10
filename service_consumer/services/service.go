package services

import (
	"github.com/sing3demons/service-consumer/database"
	"github.com/sirupsen/logrus"
)

type Service struct {
	db     database.IMongo
	logger *logrus.Logger
}

func NewService(db database.IMongo, logger *logrus.Logger) *Service {
	return &Service{db, logger}
}

func (svc *Service) InsertProduct(topic string, msg []byte, timestamp string) {

	// svc.logger.WithFields(logrus.Fields{
	// 	"timestamp": timestamp,
	// 	"result":    "",
	// 	"headers":   event.Header,
	// }).Info("Insert Product")
}
