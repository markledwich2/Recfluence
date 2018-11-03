import {layoutTextLabel,layoutRemoveOverlaps,layoutGreedy,layoutLabel} from "d3fc-label-layout";


function labelLayout() {
    const labelPadding = 2;

    // the component used to render each label
    const textLabel = layoutTextLabel()
        .padding(labelPadding)
        .value(d => d.properties.name);

    // a strategy that combines simulated annealing with removal
    // of overlapping labels
    const strategy = layoutRemoveOverlaps(layoutGreedy());

    // create the layout that positions the labels
    const labels = layoutLabel(strategy)
    .size((d, i, g) => {
        // measure the label and add the required padding
        const textSize = g[i].getElementsByTagName('text')[0].getBBox();
        return [textSize.width + labelPadding * 2, textSize.height + labelPadding * 2];
    })
    .position(d => [0,0])
    .component(textLabel);
}
