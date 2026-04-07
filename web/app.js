const data = window.CATALOG_DATA || { categories: [], products: [] };

let currentCategory = "全部";
let currentPage = 1;
const PAGE_SIZE = 40;

function formatPrice(product) {
  if (product.final_price_cny) {
    return {
      main: `¥${product.final_price_cny}`,
      mainClass: "price-discount",
      original: product.original_price_cny ? `¥${product.original_price_cny}` : "",
    };
  }

  return {
    main: product.price_cny ? `¥${product.price_cny}` : "暂无价格",
    mainClass: "price-current",
    original: "",
  };
}

function filterProducts() {
  const products = currentCategory === "全部"
    ? data.products.slice()
    : data.products.filter((product) => product.category === currentCategory);

  products.sort((a, b) => {
    const hotDiff = Number(b.hot_index || 0) - Number(a.hot_index || 0);
    if (hotDiff !== 0) return hotDiff;
    return String(a.name || "").localeCompare(String(b.name || ""), "zh-CN");
  });

  return products;
}

function renderFilters() {
  const root = document.getElementById("categoryFilters");
  root.innerHTML = "";

  data.categories.forEach((category) => {
    const button = document.createElement("button");
    button.className = "filter-chip" + (category.name === currentCategory ? " is-active" : "");
    button.type = "button";
    button.textContent = `${category.name} (${category.count})`;
    button.addEventListener("click", () => {
      currentCategory = category.name;
      currentPage = 1;
      render();
    });
    root.appendChild(button);
  });
}

function createImageNode(product) {
  if (product.image) {
    const img = document.createElement("img");
    img.className = "product-image";
    img.src = product.image;
    img.alt = product.name;
    img.loading = "lazy";
    img.onerror = () => {
      img.replaceWith(createFallbackNode());
    };
    return img;
  }
  return createFallbackNode();
}

function createFallbackNode() {
  const fallback = document.createElement("div");
  fallback.className = "image-fallback";
  fallback.textContent = "No Image";
  return fallback;
}

function renderProducts() {
  const products = filterProducts();
  const totalPages = Math.max(1, Math.ceil(products.length / PAGE_SIZE));
  if (currentPage > totalPages) {
    currentPage = totalPages;
  }
  const startIndex = (currentPage - 1) * PAGE_SIZE;
  const pageProducts = products.slice(startIndex, startIndex + PAGE_SIZE);

  const root = document.getElementById("productGrid");
  const title = document.getElementById("sectionTitle");
  const summary = document.getElementById("sectionSummary");

  title.textContent = currentCategory;
  summary.textContent = `${products.length} 个商品 · 第 ${currentPage} / ${totalPages} 页`;
  root.innerHTML = "";

  if (!pageProducts.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "当前分类下没有可展示的商品。";
    root.appendChild(empty);
    renderPagination(products.length, totalPages);
    return;
  }

  pageProducts.forEach((product) => {
    const price = formatPrice(product);

    const card = document.createElement("article");
    card.className = "product-card";

    const link = document.createElement("a");
    link.className = "product-link";
    link.href = product.url || "#";
    link.target = "_blank";
    link.rel = "noreferrer";

    const imageWrap = document.createElement("div");
    imageWrap.className = "product-image-wrap";
    imageWrap.appendChild(createImageNode(product));

    const badge = document.createElement("div");
    badge.className = "status-badge";
    badge.textContent = product.status || "unknown";
    imageWrap.appendChild(badge);

    const body = document.createElement("div");
    body.className = "product-body";

    const category = document.createElement("p");
    category.className = "product-category";
    category.textContent = product.category;

    const name = document.createElement("h3");
    name.className = "product-name";
    name.textContent = product.name || "未命名商品";

    const priceBlock = document.createElement("div");
    priceBlock.className = "price-block";

    const priceMain = document.createElement("div");
    priceMain.className = "price-main";

    const current = document.createElement("span");
    current.className = price.mainClass;
    current.textContent = price.main;
    priceMain.appendChild(current);

    if (price.original) {
      const original = document.createElement("span");
      original.className = "price-original";
      original.textContent = price.original;
      priceMain.appendChild(original);
    }

    const stock = document.createElement("p");
    stock.className = "stock-text";
    stock.textContent = `库存: ${product.stock || "暂无库存信息"}`;

    const hot = document.createElement("p");
    hot.className = "hot-text";
    hot.textContent = product.hot_index
      ? `热门指数: ${product.hot_index} · 排名 ${product.hot_rank || "-"}`
      : "热门指数: 0";

    const updated = document.createElement("p");
    updated.className = "update-text";
    updated.textContent = product.updated_at
      ? `最近更新: ${product.updated_at}`
      : "最近更新: 暂无";

    priceBlock.appendChild(priceMain);
    body.appendChild(category);
    body.appendChild(name);
    body.appendChild(priceBlock);
    body.appendChild(stock);
    body.appendChild(hot);
    body.appendChild(updated);

    link.appendChild(imageWrap);
    link.appendChild(body);
    card.appendChild(link);
    root.appendChild(card);
  });

  renderPagination(products.length, totalPages);
}

function renderPagination(totalCount, totalPages) {
  const root = document.getElementById("pagination");
  root.innerHTML = "";

  if (totalCount <= PAGE_SIZE) {
    return;
  }

  const prev = document.createElement("button");
  prev.className = "page-button";
  prev.type = "button";
  prev.textContent = "上一页";
  prev.disabled = currentPage <= 1;
  prev.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage -= 1;
      renderProducts();
    }
  });

  const indicator = document.createElement("div");
  indicator.className = "page-indicator";
  indicator.textContent = `第 ${currentPage} 页 / 共 ${totalPages} 页`;

  const next = document.createElement("button");
  next.className = "page-button";
  next.type = "button";
  next.textContent = "下一页";
  next.disabled = currentPage >= totalPages;
  next.addEventListener("click", () => {
    if (currentPage < totalPages) {
      currentPage += 1;
      renderProducts();
    }
  });

  root.appendChild(prev);
  root.appendChild(indicator);
  root.appendChild(next);
}

function renderStats() {
  document.getElementById("categoryCount").textContent = Math.max(data.categories.length - 1, 0);
  document.getElementById("productCount").textContent = data.products.length;
}

function render() {
  renderStats();
  renderFilters();
  renderProducts();
}

render();
