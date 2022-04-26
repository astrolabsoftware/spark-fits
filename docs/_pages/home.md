---
layout: splash
permalink: /
header:
  overlay_color: "#376293"
  overlay_image: /assets/images/background.jpg
  cta_label: "<i class='fas fa-download'></i> Install Now"
  cta_url: "/docs/installation/"
  caption:
intro:
  - excerpt: '<p><font size="6">Distribute FITS data with Apache Spark: Binary tables, images and more!</font></p><br /><a href="https://github.com/astrolabsoftware/spark-fits/releases/tag/1.0.0">Latest release: 1.0.0</a>'
excerpt: '{::nomarkdown}<iframe style="display: inline-block;" src="https://ghbtns.com/github-btn.html?user=astrolabsoftware&repo=spark-fits&type=star&count=true&size=large" frameborder="0" scrolling="0" width="160px" height="30px"></iframe> <iframe style="display: inline-block;" src="https://ghbtns.com/github-btn.html?user=astrolabsoftware&repo=spark-fits&type=fork&count=true&size=large" frameborder="0" scrolling="0" width="158px" height="30px"></iframe>{:/nomarkdown}'
feature_row:
  - image_path:
    alt:
    title: "<i class='fas fa-edit'></i> API for Scala, Python, Java and R"
    excerpt: "All APIs share the same core classes and routines, so the ways to create DataFrame from all languages using spark-fits are identical."
    url: "/docs/api/"
    btn_class: "btn--primary"
    btn_label: "Learn More"
  - image_path:
    alt:
    title: "<i class='fas fa-terminal'></i> Load and distribute data interactively"
    excerpt: "Load your favourite binary tables and images data and distribute the data across machines using the spark-shell, pyspark, or jupyter notebook!"
    url: "/docs/interactive/"
    btn_class: "btn--primary"
    btn_label: "Learn More"
  - image_path:
    alt:
    title: "<i class='fas fa-code'></i> Include spark-fits in your application"
    excerpt: "Include spark-fits in your next projects!"
    url: "/docs/batch/"
    btn_class: "btn--primary"
    btn_label: "Learn More"
---

{% include feature_row id="intro" type="center" %}

{% include feature_row %}
