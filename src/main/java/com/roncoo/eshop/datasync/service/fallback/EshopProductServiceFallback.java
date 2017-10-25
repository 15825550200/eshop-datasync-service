package com.roncoo.eshop.datasync.service.fallback;

import org.springframework.stereotype.Component;

import com.roncoo.eshop.datasync.service.EshopProductService;

@Component
public class EshopProductServiceFallback implements EshopProductService{

	@Override
	public String findBrandById(Long id) {
		return null;
	}

	@Override
	public String findBrandByIds(String ids) {
		return null;
	}

	@Override
	public String findCategoryById(Long id) {
		return null;
	}

	@Override
	public String findProductIntroById(Long id) {
		return null;
	}

	@Override
	public String findProductPropertyById(Long id) {
		return null;
	}

	@Override
	public String findProductById(Long id) {
		return null;
	}

	@Override
	public String findProductSpecificationById(Long id) {
		return null;
	}

}
