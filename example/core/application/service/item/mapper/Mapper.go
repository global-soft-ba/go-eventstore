package mapper

import (
	readModel "example/core/port/readmodel/item"
	service "example/core/port/service/item"
)

func FromReadModelToServiceDTOs(dtos []readModel.DTO) []service.DTO {
	var serviceDTOs []service.DTO
	for _, dto := range dtos {
		serviceDTOs = append(serviceDTOs, FromReadModelToServiceDTO(dto))
	}
	return serviceDTOs
}

func FromReadModelToServiceDTO(dto readModel.DTO) service.DTO {
	return service.DTO{
		TenantID: dto.TenantID,
		ItemID:   dto.ItemID,
		ItemName: dto.ItemName,
	}
}
